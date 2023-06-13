import numpy as np
import MITgcmDiff.Operators as op
import MITgcmDiff.xarrayCoversion as xac

# Matt's 
# rhoConst = 1027.5
# c_p      = 3994.0

# Rui's
rhoConst = 1035.0
c_p      = 3994.0


def Qsw_shape(z):
    return ( 0.62 * np.exp(z/0.6) + (1 - 0.62) * np.exp(z/20.0) ) * (z >= -200.0)

def SFCFLX_shape(z):
    return z >= 0


def cvt2Dto3D(a, coo):
    return np.expand_dims(a, 0)

def computeHeatTendency(
    d,
    coo,
    return_xarray = False,
):
    _d = dict()

    _d["TOTTTEND"] = d["TOTTTEND"] / 86400.0

    # oceQnet = EXFls + EXFlh - (EXFlwnet + EXFswnet)
    # oceQsw = - EXFswnet

    # EXFqnet = - oceQnet = EXFlwnet + EXFswnet - EXFlh - EXFhs
    # TFLUX = surForcT + oceQsw + oceFreez + [PmEpR*SST]*Cp
    # oceFWflx = [PmEpR]

    # In our case where sea ice does not involve
    # TFLUX = oceQsw + EXFhl + EXFhs - EXFlwnet + [PmEpR*SST]*Cp
    #                                                  |
    #                                                  +--> the loss/gain of ocean mass * c_p

    _d["SWFLX"]  = - cvt2Dto3D(d["oceQsw"], coo)              * Qsw_shape(coo.grid["RF"][:-1])
    _d["SFCFLX"] = - cvt2Dto3D(d["TFLUX"] - d["oceQsw"], coo) * SFCFLX_shape(coo.grid["RF"][:-1])
    

    _d["LWFLX"]  =   cvt2Dto3D(d["EXFlwnet"], coo) * SFCFLX_shape(coo.grid["RF"][:-1])
    _d["SENFLX"] = - cvt2Dto3D(d["EXFhs"], coo)        * SFCFLX_shape(coo.grid["RF"][:-1])
    _d["LATFLX"] = - cvt2Dto3D(d["EXFhl"], coo)        * SFCFLX_shape(coo.grid["RF"][:-1])
    _d["FRZFLX"] = - cvt2Dto3D(d["oceFreez"], coo)   * SFCFLX_shape(coo.grid["RF"][:-1])
    _d["RLXFLX"] = - cvt2Dto3D(d["TRELAX"], coo)     * SFCFLX_shape(coo.grid["RF"][:-1])
    

    oceQnet = d["oceQsw"] - d["EXFlwnet"] + d["EXFhl"] + d["EXFhs"]

    TDI = d["surForcT"] - oceQnet - d['TRELAX'] + d["oceQsw"] 
    _d["TDIFLX"] = - cvt2Dto3D(TDI, coo) * SFCFLX_shape(coo.grid["RF"][:-1])
    
    MAS = d["TFLUX"] - d["surForcT"] - d["oceQsw"] - d["oceFreez"]
    _d["MASFLX"] = - cvt2Dto3D(MAS, coo) * SFCFLX_shape(coo.grid["RF"][:-1])
    
    _d["WTHMASS_masked"] = d["WTHMASS"] * SFCFLX_shape(coo.grid["RF"][:-1])

    _d["TEND_SWFLX"]  = - op.T_DIVz_W(_d["SWFLX"], coo, weighted=False)  / (rhoConst * c_p)
    _d["TEND_LWFLX"]  = - op.T_DIVz_W(_d["LWFLX"], coo, weighted=False)  / (rhoConst * c_p)
    _d["TEND_LATFLX"] = - op.T_DIVz_W(_d["LATFLX"], coo, weighted=False) / (rhoConst * c_p)
    _d["TEND_SENFLX"] = - op.T_DIVz_W(_d["SENFLX"], coo, weighted=False) / (rhoConst * c_p)
    _d["TEND_FRZFLX"] = - op.T_DIVz_W(_d["FRZFLX"], coo, weighted=False) / (rhoConst * c_p)
    _d["TEND_TDIFLX"] = - op.T_DIVz_W(_d["TDIFLX"], coo, weighted=False) / (rhoConst * c_p)
    _d["TEND_RLXFLX"] = - op.T_DIVz_W(_d["RLXFLX"], coo, weighted=False) / (rhoConst * c_p)
    _d["TEND_MASFLX"] = - op.T_DIVz_W(_d["MASFLX"], coo, weighted=False) / (rhoConst * c_p)

    _d["TEND_SFCFLX"] = - op.T_DIVz_W(_d["SFCFLX"], coo, weighted=False) / (rhoConst * c_p)
    _d["TEND_SFC_WTHMASS"] = - op.T_DIVz_W(_d["WTHMASS_masked"], coo, weighted=False)
 

    # Verifying oceQnet
    _d["oceQnet"]    = - cvt2Dto3D(d["oceQnet"], coo)  * SFCFLX_shape(coo.grid["RF"][:-1])
    _d["my_oceQnet"] = - cvt2Dto3D(oceQnet,      coo)  * SFCFLX_shape(coo.grid["RF"][:-1])
   
    _d["oceQnet"]    = - op.T_DIVz_W(_d["oceQnet"],    coo, weighted=False) / (rhoConst * c_p)
    _d["my_oceQnet"] = - op.T_DIVz_W(_d["my_oceQnet"], coo, weighted=False) / (rhoConst * c_p)

    _d["DIFF_oceQnet"] = _d["oceQnet"] - _d["my_oceQnet"]



    _d["TEND_ADVx"] = - (
            op.T_DIVx_U(d["ADVx_TH"], coo, weighted=True)
    )

    _d["TEND_ADVy"] = - (
            op.T_DIVy_V(d["ADVy_TH"], coo, weighted=True)
    )

    _d["TEND_ADVz"] = - (
            op.T_DIVz_W(d["ADVr_TH"], coo, weighted=True)
    )

    _d["TEND_ADV"] = _d["TEND_ADVx"] + _d["TEND_ADVy"] + _d["TEND_ADVz"]


    _d["TEND_DIFFz"] =  - (
             op.T_DIVz_W(d["DFrE_TH"], coo, weighted=True)
           + op.T_DIVz_W(d["DFrI_TH"], coo, weighted=True)
           + op.T_DIVz_W(d["KPPg_TH"], coo, weighted=True)
    )

    _d["TEND_DIFFx"] = - (
            op.T_DIVx_U(d["DFxE_TH"], coo, weighted=True)
    )

    _d["TEND_DIFFy"] = - (
            op.T_DIVy_V(d["DFyE_TH"], coo, weighted=True)
    )



    _d["TEND_SUM"] = (

           _d["TEND_ADVx"]
         + _d["TEND_ADVy"]
         + _d["TEND_ADVz"]
         + _d["TEND_DIFFx"]
         + _d["TEND_DIFFy"]
         + _d["TEND_DIFFz"]

         + _d["TEND_SWFLX"]
         + _d["TEND_LWFLX"]
         + _d["TEND_SENFLX"]
         + _d["TEND_LATFLX"]
         + _d["TEND_FRZFLX"]
         + _d["TEND_TDIFLX"]
         + _d["TEND_RLXFLX"]
         + _d["TEND_MASFLX"]

         + _d["TEND_SFC_WTHMASS"]
         #+ _d["TEND_SFCFLX"]
    )

    _d["TEND_RES"] = _d["TEND_SUM"] - _d["TOTTTEND"]

    if return_xarray:
        
        dict_dimarr = {
                varname : ("T", vardata) for varname, vardata in _d.items()
        }


        ds = xac.mkDataset(
            dict_dimarr = dict_dimarr,
            coo = coo,
        )

        return ds

    else:
        return _d



def computeSaltTendency(
    d,
    coo,
    return_xarray = False,
):
    _d = dict()

    _d["TOTSTEND"] = d["TOTSTEND"] / 86400.0

    _d["SFCFLX"] = - cvt2Dto3D(d["SFLUX"], coo) * SFCFLX_shape(coo.grid["RF"][:-1])
    _d["WSLTMASS_masked"] = d["WSLTMASS"] * SFCFLX_shape(coo.grid["RF"][:-1])

    

    print("Shape of mask")
    print(_d["WSLTMASS_masked"].shape)

    #print(Qsw_shape(coo.grid["RF"][:-1])) 
    print("Compute ADVx_SLT")
    #print(d["ADVx_TH"].shape)
    #print(coo.grid["DVOLT"].shape)
    _d["TEND_ADVx"] = - (
            op.T_DIVx_U(d["ADVx_SLT"], coo, weighted=True)
    )

    print("Compute ADVy_TH")
    _d["TEND_ADVy"] = - (
            op.T_DIVy_V(d["ADVy_SLT"], coo, weighted=True)
    )

    print("Compute ADVz_TH")
    _d["TEND_ADVz"] = - (
            op.T_DIVz_W(d["ADVr_SLT"], coo, weighted=True)
    )


    _d["TEND_ADV"] = _d["TEND_ADVx"] + _d["TEND_ADVy"] + _d["TEND_ADVz"]


    _d["TEND_DIFFz"] = - (
           op.T_DIVz_W(d["DFrI_SLT"], coo, weighted=True)
    )

    
    _d["TEND_SFCFLX"] = - op.T_DIVz_W(_d["SFCFLX"], coo, weighted=False) / rhoConst
    
    _d["TEND_SFC_WSLTMASS"] = - op.T_DIVz_W(_d["WSLTMASS_masked"], coo, weighted=False)

    _d["TEND_SUM"] = (
        _d["TEND_ADVx"]
         + _d["TEND_ADVy"]
         + _d["TEND_ADVz"]
         + _d["TEND_DIFFz"]
         + _d["TEND_SFCFLX"]
         + _d["TEND_SFC_WSLTMASS"]
    )

    _d["TEND_RES"] = _d["TEND_SUM"] - _d["TOTSTEND"]

    if return_xarray:
        
        dict_dimarr = {
                varname : ("T", vardata) for varname, vardata in _d.items()
        }


        ds = xac.mkDataset(
            dict_dimarr = dict_dimarr,
            coo = coo,
        )

        return ds

    else:
        return _d




