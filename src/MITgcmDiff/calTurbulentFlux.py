import numpy as np
import MITgcmDiff.Operators as op
import MITgcmDiff.Interpolate as itp
import MITgcmDiff.xarrayCoversion as xac
import MITgcmDiff.Coordinate
# Matt's 
# rhoConst = 1027.5
# c_p      = 3994.0

# Rui's
rhoConst = 1035.0
c_p      = 3994.0



def computeTurbulentFlux(adv, vel, tracer, coo: MITgcmDiff.Coordinate, direction="Z"):

    if direction == "Z":
        # It is assumed vel = WVEL and is on W-grid
        # interpolate tracer onto W grid
        tracer_W = itp.W_interp_T(tracer, coo)
        turbflx = adv - vel * tracer

    else:
        
        raise Exception("Unknown `direction`: %s" % (direction,))

    return turbflx





