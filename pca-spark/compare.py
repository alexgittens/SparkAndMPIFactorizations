import numpy as np, umsgpack, gzip

exactfin = gzip.open("data/eofs-centerOverAllObservations-both-2.binexact", "r")
inexactfin = gzip.open("data/eofs-centerOverAllObservations-both-2.bininexact", "r")

def loadEofs(fin):
  """ Loads flattened Breeze eofs stored by msgpack from a given file handle.
  Note that Breeze and Numpy use different major order for storing matrices """
  numcols = umsgpack.unpack(fin)
  numrows = umsgpack.unpack(fin)
  listofvals = umsgpack.unpack(fin)
  u = np.array(listofvals).reshape(numcols, len(listofvals)/numcols)
  U = np.matrix(u.T)

  listofvals = umsgpack.unpack(fin)
  v = np.array(listofvals).reshape(numcols, len(listofvals)/numcols)
  V = np.matrix(v.T)

  s = np.array(umsgpack.unpack(fin))

  return [U, s, V]

exactEofs = loadEofs(exactfin)
inexactEofs = loadEofs(inexactfin)

print "U frobenius difference: ", np.linalg.norm(exactEofs[0] - inexactEofs[0])
print "V frobenius difference: ", np.linalg.norm(exactEofs[2] - inexactEofs[2])
print "S l2 distance: ", np.linalg.norm(exactEofs[1] - inexactEofs[1])



