from numpy.linalg import svd, norm
from numpy.random import randn
import numpy as np
import sys

def gen_unif_bad_mat(m,n):

    cond = 1e6

    U, = svd(randn(m,min(m,n)),False)[:1]
    s  = np.linspace(1,cond,min(m,n))
    V, = svd(randn(n,min(m,n)),False)[:1]

    #A   = (U*s).dot(V.T)
    A   = np.dot((U*s),V.T)
    b   = np.dot(A,randn(n))
    res = randn(m)
    theta = 0.25;
    b    += theta*norm(b) * res/norm(res)

    #x_opt = V.dot(U.T.dot(b)/s)
    #x_opt = np.dot(V,np.dot(U.T,b)/s)

    return A, b

if __name__ == "__main__":
    m = int(sys.argv[1])
    n = int(sys.argv[2])

    filename = 'unif_bad_'+str(m)+'_'+str(n)

    A,b = gen_unif_bad_mat(m,n)
    np.savetxt(filename+'.txt',A,fmt='%.12e')

    #if len(sys.argv) > 3: 
    #    U, D, V = np.linalg.svd(A,0)
    #    np.savetxt(filename+'_U.txt',U,fmt='%.12e')
    #    np.savetxt(filename+'_D.txt',D,fmt='%.12e')
    #    np.savetxt(filename+'_V.txt',V,fmt='%.12e')

    #x_opt = np.linalg.lstsq(A,b)[0]
    #f_opt = norm(np.dot(A,x_opt)-b)
    #Ab = np.hstack((A,b.reshape((m,1))))
    #np.savetxt(filename+'_Ab.txt',Ab,fmt='%.12e')

    #np.savetxt(filename+'_b.txt',b)
    #np.savetxt(filename+'_x_opt.txt',x_opt)
    #np.savetxt(filename+'_f_opt.txt',np.array([f_opt]))

