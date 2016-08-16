#!/usr/bin/env python
# NB: if you write to local HDFS, ensure that the directories
# "<userdir>/CFSROcsv/vals" and "<userdir>/CFSROcsv/recordDateMapping" exist
#
# This code converts a GRIB climate dataset stored on S3 to csv and either writes it to local HDFS or back to S3.
# It can be run simultaneously in several processes on several machines, to facilitate processing large amounts of data,
# and can be stopped and restarted (assuming it isn't stopped while in the process of actually writing a file) without
# reprocessing or missing processing of any of the GRIB files.
#
# call with convertGRIBtoCSV.py AWS_KEY AWS_SECRET_KEY NUMPROCESSES MYREMAINDER (see below for explanation)

from boto.s3.connection import S3Connection
from boto.s3.key import Key
import numpy as np
import pydoop.hdfs as hdfs
import pygrib
import gzip
from datetime import datetime
import os, sys

# output_to_S3_flag: false, if want output to be stored on local HDFS
# compressed_flag:  if true, output will be gzip compressed
# numprocesses needs to be prime, to ensure only one process tries to process each record
# myremainder should be a unique number between 0 and process - 1 that identifies the records this process will attempt to convert

def convertGRIBs(aws_key, aws_secret_key, numprocesses, myremainder, compressed_flag = True, output_to_S3_flag = False):

    tempdir = "/mnt3/ubuntu" # for r3.8xlarge instances, assumes this is linked to one of the SSDs
    #tempdir = "/tmp" # for other instances

    conn = S3Connection(aws_key, aws_secret_key)

    # source of the CFSRO-O data, as a set of grb2 files
    bucket = conn.get_bucket('agittens')
    keys = bucket.list(prefix='CSFR-O/grib2/ocnh06.gdas') # should manually enforce a sorting on these so you know the explicit map between the record number and a particular sample observation

    # make these vectors global because they're huge, so don't want to reallocate them 
    dimsperlevel = 360*720
    dims = dimsperlevel * 41
    vals = np.zeros((dims,))
    mask = np.zeros((dims,)) < 0

    # returns the set of gribs from an s3 key 
    tempgribfname = tempdir + '/temp{0}'.format(myremainder)
    def get_grib_from_key(inkey):
        with open(tempgribfname, 'w') as fin:
            inkey.get_file(fin)
        return pygrib.open(tempgribfname.format(myremainder))

    # index of the gribs within the grib file that correspond to SST and sub surface sea temperatures
    gribindices = range(1,41)
    gribindices.append(207)
    gribindices = list(reversed(gribindices))

    # for a given set of gribs, extracts the desired temperature observations and converts them to a vector of
    # observations and drops missing observations
    def converttovec(grbs):

        for index in range(41):
            maskedobs = grbs[gribindices[index]].data()[0]
            vals[index*dimsperlevel:(index+1)*dimsperlevel] = maskedobs.data.reshape((dimsperlevel,))
            mask[index*dimsperlevel:(index+1)*dimsperlevel] = maskedobs.mask.reshape((dimsperlevel,))
            
        return vals[mask == False]

    # prints a given status message with a timestamp
    def report(status):
        print datetime.now().time().isoformat() + ":\t" + status

    # convenience function so can write to a compressed or uncompressed file transparently
    if compressed_flag:
        myopen = gzip.open
    else:
        myopen = open
        
    error_fh = open('grib_conversion_error_log_{0}_of_{1}'.format(myremainder, numprocesses), 'w')

    recordDateMapping = {}
    mappingfname = "CFSROcsv/recordDateMapping/part-" + format(myremainder, "05")
    if compressed_flag:
        mappingfname += ".gz"

    for (recordnum, inkey) in enumerate(keys):
        # only process records assigned to you
        if (recordnum % numprocesses) is not myremainder:
            continue
        recordDateMapping[recordnum] = inkey.name.split('.')[2]

        # choose the right name for the vector of observations and the vector of masks
        # depending on whether or not they're compressed
        valsfname = "CFSROcsv/vals/part-"+format(recordnum,"05")
        if compressed_flag:
            valsfname += ".gz"
            
        # avoid processing this set of observations if it has already been converted
        if output_to_S3_flag:
            possible_key = bucket.get_key(valsfname)
            if possible_key is not None:
                report("{0} already converted to csv, skipping record {1}".format(inkey.name, recordnum))
                continue
        else:
            if hdfs.path.isfile(valsfname):
                report("{0} already converted to csv, skipping record {1}".format(inkey.name, recordnum))
                continue
                
        # convert the observations and write them out to HDFS/S3 compressed/uncompressed
        try:
            grbs = get_grib_from_key(inkey)
            report("Retrieved {0} from S3".format(inkey.name))
        
            observations = converttovec(grbs)
            report("Converted {0} to a numpy array of observations".format(inkey.name))
        
            tempvalsfname = tempdir + '/tempvals{0}'.format(myremainder)
            with myopen(tempvalsfname, 'w') as valsfout:
	        for index in range(0, observations.shape[0]):
		    valsfout.write("{0},{1},{2}\n".format(recordnum, index, observations[index]))
            report("Wrote numpy array to a local file")
            
            if output_to_S3_flag:
                valsoutkey = Key(bucket)
                valsoutkey.key = valsfname
                valsoutkey.set_contents_from_filename(tempvalsfname)
                report("Wrote {0} to {1} on S3".format(inkey.name.split('.')[2], valsfname))
            else:
                hdfs.put(tempvalsfname, valsfname)
                report("Wrote {0} to {1} on HDFS".format(inkey.name.split('.')[2], valsfname))
        except:
            report("Skipping record {0}! An error occurred processing {1}".format(recordnum, inkey.name))
            error_fh.write("Skipped {1}, record {0}\n".format(inkey.name, recordnum))
            
    try:
        os.remove(tempgribfname)
        os.remove(tempvalsfname)

        # write the record mapping out to file so we know which rows correspond to which date
    	temprecordfname = tempdir + '/temp{0}recordmapping'.format(myremainder)
        with myopen(temprecordfname, 'w') as fout:
            for recordnum, keyname in recordDateMapping.iteritems():
                fout.write("{0},{1}\n".format(recordnum, keyname))
        report("Wrote the observation date to row number mapping for process {0} to local file".format(myremainder))
             
        if output_to_S3_flag:
            mappingoutkey = Key(bucket)
            mappingoutkey.key = mappingfname
            mappingoutkey.set_contents_from_filename(temprecordfname)
            report("Wrote record mapping for {0} to {1} on S3".format(myremainder, mappingfname))
        else:
            hdfs.put(temprecordfname, mappingfname)
            report("Wrote record mapping for {0} to {1} on HDFS".format(myremainder, mappingfname))

        os.remove(temprecordfname)
    except:
        report("Skipping writing the record mapping for {0}! An error occurred writing it out.".format(myremainder))
        error_fh.write("Skipping writing the record mapping for {0}! An error occurred writing it out.\n".format(myremainder))

    error_fh.close()

if __name__ == "__main__":
    convertGRIBs(sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4]))
