from h5py import File
import numpy as np

# about 3 GB
m = int(2e6) 
n = 200
k = 100
r = 20

W = np.random.random((m,r))
H = np.zeros((r, n))
H[:, :r] = np.eye(r)
H[:, r:] = np.random.random((r, n-r))
for i in np.arange(2, 20, 1):
    temp = H[:, i]
    H[:, i] = H[:, 10*i]
    H[:, 10*i] = temp

fout = File("testdata.h5", "w")
fout.create_dataset("mat", data=W.dot(H))
fout.close()



