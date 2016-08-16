import numpy as np

def gen_nonunif_bad_mat(m,n):
    num_ones = min(n/2,20)
    a1 = np.hstack((50*np.random.randn(m-num_ones,n-num_ones),1e-8*np.random.randn(m-num_ones,num_ones)))
    a2 = np.hstack((np.zeros((num_ones,n-num_ones)),np.eye(num_ones)))
    A = np.vstack((a1,a2))
    x = np.random.randn(n)
    b = np.dot(A,x)
    err = np.random.randn(m)
    b = b + 0.25*np.linalg.norm(b)/np.linalg.norm(err)*err

    return A,b

if __name__ == "__main__":
    m = 10000
    n = 500

    filename = 'nonunif_bad_'+str(m)+'_'+str(n)

    A,b = gen_nonunif_bad_mat(m,n)
    #x_opt = np.linalg.lstsq(A,b)[0]
    #f_opt = np.linalg.norm(np.dot(A,x_opt)-b)
    #Ab = np.hstack((A,b.reshape((m,1))))

    #np.savetxt(filename+'_Ab.txt',Ab,fmt='%.12e')
    np.savetxt(filename+'.txt',A,fmt='%.12e')
    #np.savetxt(filename+'_b.txt',b)
    #np.savetxt(filename+'_x_opt.txt',x_opt)
    #np.savetxt(filename+'_f_opt.txt',np.array([f_opt]))

