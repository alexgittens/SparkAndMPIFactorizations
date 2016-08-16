# NB: if you change the preprocessing done before computing the EOFs, you need
#to edit the headers below for the NetCDF file

# Assuming EOFs have been converted from Binary to CSV format, converts them
# to netCDF CDL specification to be compiled to netCDF using ncgen. Note: this
# was necessary because it's not clear to me how to use PyNetCDF to directly
# generate a netcdf file with the structure I want. Once the CDL file has been
# compiled to NetCDF, one could maybe load this back in and look at its structure
# to determine how to create it directly...

# to compile the CDL file to netCDF, use
# ncgen -v 2 -o <outfname> <CDLfname>
# the -v 2 argument tells it to use 64bit offsets (otherwise you can't generate files larger than 2Gb)

import numpy as np
from os.path import join
import time

numEofs = 100
numObservedGridPoints = 6349676
numDates = 46715
dataDirectory = '/global/cscratch1/sd/gittens/large-scale-climate/data'
masktemplatefname = '/global/cscratch1/sd/gittens/CFSROparquet/masktemplate.npy'
latsfname = '/global/cscratch1/sd/gittens/CFSROparquet/lats.npy'
lonsfname = '/global/cscratch1/sd/gittens/CFSROparquet/lons.npy'
coldatafname = '/global/cscratch1/sd/gittens/CFSROparquet/coldates'

# loads the mask indicating what grid points were not observed,  
# the list of the dates each column in the right EOFs correspond to,
# and the latitudes and longitude points
templatemask = np.load(masktemplatefname)
coldates = [line.rstrip('\n') for line in open(coldatafname)]
lats = np.load(latsfname)
lons = np.load(lonsfname)

dimsperlevel = 360*720
dims = dimsperlevel * 41
mask = np.zeros((dims,)) < 0

for index in range(41):
    mask[index*dimsperlevel:(index+1)*dimsperlevel] = templatemask[index,...].reshape((dimsperlevel,))

# hard-coded: the depth of the observations within the ocean (m)
levels = np.array([0, 5, 15, 25, 35, 45, 55, 65, 75, 85, 95, 105, 115, 125, 135, \
                   145, 155, 165, 175, 185, 195, 205, 215, 225, 238, 262, 303, \
                   366, 459, 584, 747, 949, 1193, 1479, 1807, 2174, 2579, 3016, \
                   3483, 3972, 4478])

# load the EOFs from the CSV files
def loadEOF(fname, numeofs, numobs):
    eofs = np.zeros((numeofs, numobs))
    with open(fname, 'r') as fin:
        fin.readline() # skip the two header lines
        fin.readline()
        for line in fin:
            (i, j, v) = line.split()
            eofs[int(j)-1, int(i)-1] = float(v)
    return eofs

def loadVec(fname, numeofs):
    vec = np.zeros((numeofs,))
    with open(fname, 'r') as fin:
        fin.readline()
        fin.readline()
        for line in fin:
            (i,j,v) = line.split()
            vec[int(j)-1] = float(v)
    return vec

def fullEOF(eof):
    eofpadded = 9999*np.ones((dims,))
    eofpadded[mask == False] = eof
    return eofpadded

print "Loading the eigenvalues from CSV"
singvals = loadVec(join(dataDirectory, "evalEOFs"), numEofs)
print "Loading the row means from CSV"
observedmeans = loadVec(join(dataDirectory, "rowMeans"), numObservedGridPoints)
print "Loading the column EOFs from CSV"
observedeofs = loadEOF(join(dataDirectory, "colEOFs"), numEofs, numObservedGridPoints)
print "Loading the row EOFs from CSV"
temporaleofs = loadEOF(join(dataDirectory, "rowEOFs"), numEofs, numDates)

spatialeofs = np.zeros((numEofs, dims))
for idx in range(numEofs):
    spatialeofs[idx, :] = fullEOF(observedeofs[idx, :])
means = fullEOF(observedmeans)

# write out the CDL file
eofs_ncdump_template_header = \
"""
netcdf {0} {{
    //CDL specification for EOF file, cf https://www.unidata.ucar.edu/software/netcdf/docs/ncgen-man-1.html for format specification
    
    dimensions:
        lat_0 = 360 ;
        lon_0 = 720 ;
        lv_DBSL0 = 41 ;
        numeofs = {1} ;
        numdates = {2} ;
        lengthofdate = 10;
        
    variables:
        float lat_0(lat_0) ;
        float lon_0(lon_0) ;
        float lv_DBSL0(lv_DBSL0) ;
        float singvals(numeofs) ;
        
        float temporalEOFs(numeofs, numdates) ;
        char coldates(numdates, lengthofdate) ;
        
        float meanTemps(lv_DBSL0, lat_0, lon_0) ;
{3}
        :creation_date = "{4}" ;
        :Conventions = "None" ;
    
    data:
"""

spatial_eof_variable_specification_template = \
"""
        float EOF_{0}(lv_DBSL0, lat_0, lon_0) ;
            EOF_{0}:type_of_statistical_processing = "Subtracted mean then scaled by abs(cos(latitude))" ;
            EOF_{0}:level_type = "Depth below sea level (m)" ;
            EOF_{0}:grid_type = "Latitude/longitude" ;
            EOF_{0}:_FillValue = 9999. ;
"""

# outputs a 2d array to string as one long vector (row order, so that last dimension varies fastest)
# assumes the array is short enough to fit in one string
def array_to_cdl_string(varname, data, mask = None):
    outstr = "\t\t" + varname + " = "
    flatview = np.reshape(data, (np.prod(data.shape), ))
    if mask is None:
        outstr += ', '.join(map(str, flatview.tolist())) + ' ;'
    else:
        flatmask = np.reshape(mask, (np.prod(mask.shape), ))
        filtermasked = lambda (idx, val): '_' if flatmask[idx] else str(val)
        outstr += ', '.join(map(filtermasked, enumerate(flatview.tolist()))) + ';'
    return outstr

def generate_ncdump(spatialeofs, temporaleofs, singvals, means, lats, lons, levels, mask, coldates, fnameout):
    spatialeofspecifications = ""
    numeofs = spatialeofs.shape[0]
    numdates = temporaleofs.shape[1]
    for eofidx in range(spatialeofs.shape[0]):
        spatialeofspecifications += spatial_eof_variable_specification_template.format(eofidx)
    datestrings = ', '.join(map(lambda date: '"' + date + '"', coldates))
    
    with open(fnameout, "w") as fout:
        fout.write(eofs_ncdump_template_header.format(fnameout, numeofs, numdates, spatialeofspecifications, time.strftime('%c')))
        fout.write(array_to_cdl_string('singvals', singvals) + '\n')
        fout.write(array_to_cdl_string('lv_DBSL0', levels) + '\n')
        fout.write(array_to_cdl_string('lat_0', lats) + '\n')
        fout.write(array_to_cdl_string('lon_0', lons) + '\n')
        fout.write(array_to_cdl_string('temporalEOFs', temporaleofs) + '\n')
        fout.write('\t\tcoldates = ' + datestrings + ';\n' )
        fout.write(array_to_cdl_string('meanTemps', means, mask) + '\n')
        for eofidx in range(numeofs):
            fout.write(array_to_cdl_string('EOF_{0}'.format(eofidx), spatialeofs[eofidx, :], mask) + '\n')
        fout.write('}')

print "Writing the EOFs to CDL format"
generate_ncdump(spatialeofs, temporaleofs, singvals, means, lats, lons, \
                levels, mask, coldates, join(dataDirectory, 'CFSR-eofs.txt'))

