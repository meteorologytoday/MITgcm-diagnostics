import numpy as np
from MITgcmDiff.buoyancy_linear import TS2rho

default_fill_value = np.nan
default_fill_value_int = -1

# https://ecco-v4-python-tutorial.readthedocs.io/Thermal_wind.html#Viewing-and-Plotting-Density

def calddz(Q, xgcm_grid, ecco_grid, interp=False):
    dQdz = xgcm_grid.diff(Q, axis='Z', boundary='fill') #/ ecco_grid.drC

    if interp:
        dQdz = xgcm_grid.interp(dQdz, 'Z')
    
    return dQdz

def calGrad(Q, xgcm_grid, ecco_grid):

    dQdx = (xgcm_grid.diff(Q, axis="X", boundary='extend')) / ecco_grid.dxC
    dQdy = (xgcm_grid.diff(Q, axis="Y", boundary='extend')) / ecco_grid.dyC

    #dQdx.data = dQdx.values
    #dQdy.data = dQdy.values

    #_interp = xgcm_grid.interp_2d_vector({"X": dQdx, "Y": dQdy}, boundary='extend')

    dQdx = xgcm_grid.interp(dQdx, "X")#_interp["X"]
    dQdy = xgcm_grid.interp(dQdy, "Y")
    #dQdy = _interp["Y"]

    return dQdx, dQdy


def detectMLNz(h, z_W, mask=None, fill_value=default_fill_value_int):

    if mask is None:
        mask = np.ones((Ny, Nx), dtype=np.int32)

    Ny, Nx = h.shape
    Nz = len(z_W) - 1
    MLNz = np.zeros((Ny, Nx), dtype=np.int32)

    for j in range(Ny):
        for i in range(Nx):

            if mask[j, i] == 0:
                MLNz[j, i] = fill_value

            else:

                z = - h[j, i]
                for k in range(Nz):

                    if z_W[k] >= z and z >= z_W[k+1]:   # Using 2 equalities so that the last grid-box will include z = z_bottom
                        MLNz[j, i] = k
                        break
 
                    elif k == Nz-1:
                        MLNz[j, i] = k
    
    return MLNz


def evalAtMLD_W(fi, h, z_W, mask=None, fill_value=default_fill_value):
  
    Nzp1, Ny, Nx = fi.shape

    if mask is None:
        mask = np.ones((Ny, Nx), dtype=np.int32)

    fo = np.zeros((Ny, Nx))
    Nz_h = detectMLNz(h, z_W, mask=mask)
    
    dz_T = z_W[:-1] - z_W[1:]

    for j in range(Ny):
        for i in range(Nx):
            
            if mask[j, i] == 0:
                fo[j, i] = fill_value
            
            else:
                
                _Nz = Nz_h[j, i]
                _h = h[j, i]

                fo[j, i] = fi[_Nz+1, j, i] + (fi[_Nz, j, i] - fi[_Nz+1, j, i]) / dz_T[_Nz] * (- _h - z_W[_Nz+1])
   
    return fo


def evalAtMLD_T(fi, h, z_W, mask=None, fill_value=default_fill_value):
  
    Nz, Ny, Nx = fi.shape

    if mask is None:
        mask = np.ones((Ny, Nx), dtype=np.int32)

    fo = np.zeros((Ny, Nx))
    Nz_h = detectMLNz(h, z_W, mask=mask)
    
    for j in range(Ny):
        for i in range(Nx):
            
            if mask[j, i] == 0:
                fo[j, i] = fill_value
            
            else:
                
                _Nz = Nz_h[j, i]
                fo[j, i] = fi[_Nz, j, i]
   
    return fo



def computeMLMean(fi, h, z_W, mask=None, fill_value=default_fill_value):
  
    Nz, Ny, Nx = fi.shape 

    if mask is None:
        mask = np.ones((Ny, Nx), dtype=np.int32)

    dz_T = z_W[:-1] - z_W[1:]
    fo = np.zeros((Ny, Nx))

    Nz_h = detectMLNz(h, z_W, mask=mask)

    for j in range(Ny):
        for i in range(Nx):
           

            _Nz = Nz_h[j, i]
            

            if mask[j, i] == 0:
                fo[j, i] = fill_value

            else:


                _tmp = 0.0
                if _Nz > 0:
                    _tmp += np.sum(dz_T[:_Nz] * fi[:_Nz, j, i])

                _tmp += (z_W[_Nz] + h[j, i]) * fi[_Nz, j, i]
                
                fo[j, i] = _tmp / h[j, i]
    
    return fo

#
# Gradient method threshold: 5e-4 ~ 5e-2 kg/m^4. The number is referenced from the paper:
#
# Holte, J., & Talley, L. (2009). A new algorithm for finding mixed layer depths 
# with applications to Argo data and Subantarctic Mode Water formation. Journal
# of Atmospheric and Oceanic Technology, 26(9), 1920-1939.
#
def findMLD_drhodz(drhodz, z_W, threshold=1e-2, mask=None, Nz_bot=None, fill_value=default_fill_value, MLD_min=0.1):


    drhodz = - drhodz
    Nz, Ny, Nx = drhodz.shape

    if mask is None:
        mask = np.ones((Ny, Nx), dtype=np.int32)
 
    if Nz_bot is None:
        Nz_bot = np.zeros((Ny, Nx), dtype=np.int32)
        Nz_bot += Nz
        
    MLD = np.zeros((Ny, Nx))
    for j in range(Ny):
        for i in range(Nx):

            if mask[j, i] == 0:
                MLD[j, i] = fill_value
                continue 

            _Nz_bot = Nz_bot[j, i]
                
               
            #print("max DRHODR: ", np.amax(drhodz[:, j, i])) 
            #print("REDD, Nz_bot: %d" % (Nz_bot[j,i],))

            if drhodz[0, j, i] >= threshold:
                MLD[j, i] = 0.0

            else:
                for k in range(1, _Nz_bot):
                    
                    if drhodz[k, j, i] >= threshold:
                    
                        MLD[j, i] = - z_W[k-1] + (z_W[k-1] - z_W[k]) * (threshold - drhodz[k-1, j, i]) / (drhodz[k, j, i] - drhodz[k-1, j, i])
                        break



                    if k == _Nz_bot-1:  # cannot find the mixed layer depth
                        MLD[j, i] = - z_W[_Nz_bot-1]

                        
            #print("k, MLD = (%d, %f)" % (k, MLD[j, i]))

    #MLD[np.isfinite(MLD) & (MLD < MLD_min)] = MLD_min

    #print(z_W)

    # Sanity check
    if np.any(MLD[np.isfinite(MLD)] <= 0):
        raise Exception("Some MLD is negative.")

    return MLD


def findMLD_rho(rho, z_T, dev=0.03, mask=None, Nz_bot=None, fill_value=default_fill_value):


    Nz, Ny, Nx = rho.shape

    if mask is None:
        mask = np.ones((Ny, Nx), dtype=np.int32)
 
    if Nz_bot is None:
        Nz_bot = np.zeros((Ny, Nx), dtype=np.int32)
        Nz_bot += Nz
        
        
    MLD = np.zeros((Ny, Nx))
    for j in range(Ny):
        for i in range(Nx):

            if mask[j, i] == 0:
                MLD[j, i] = fill_value
                continue 

            SSrho = rho[0, j, i]
            rho_threshold = SSrho + dev

            _Nz_bot = Nz_bot[j, i]
            
            for k in range(_Nz_bot):
                
                if rho[k, j, i] >= rho_threshold:
                
                    MLD[j, i] = - z_T[k-1] + (z_T[k-1] - z_T[k]) * (rho_threshold - rho[k-1, j, i])/(rho[k, j, i] - rho[k-1, j, i]) 
                    break

                if k == _Nz_bot-1:  # cannot find the mixed layer depth
                        MLD[j, i] = - z_T[k]

    # Sanity check
    if np.any(MLD[np.isfinite(MLD)] <= 0):
        throw(ErrorException("Some MLD is negative."))

    return MLD


    

