import numpy as np
import scipy




def transformCoordinate(X, Y, beg_X, beg_Y, end_X, end_Y):

    x_hat = np.array([end_X - beg_X,  end_Y - beg_Y])
    x_hat = x_hat / np.sqrt(np.sum(x_hat ** 2))

    y_hat = np.array([ - x_hat[1], x_hat[0] ])

    new_X = np.zeros_like(X) 
    new_Y = np.zeros_like(Y) 
 
    DeltaR_X = X - beg_X
    DeltaR_Y = Y - end_Y

    new_X = x_hat[0] * DeltaR_X + x_hat[1] * DeltaR_Y
    new_Y = y_hat[0] * DeltaR_X + y_hat[1] * DeltaR_Y


    return new_X, new_Y


def transformCoordinateMatrix(X, Y, beg_X, beg_Y, end_X, end_Y, resampling_dX, dY):

    # Get the new coordinate information first
    new_X, new_Y = transformCoordinate(X, Y, beg_X, beg_Y, end_X, end_Y)

    # Then, determine the weight
    # Loop through every point
    for ind, _ np.ndenumerate(X):
        scipy.sparse.csr_array
        new_X[ind]





