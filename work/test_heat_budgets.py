import numpy as np
import xarray as xr
import MITgcmDiff.loadFunctions as lf
from MITgcmutils import mds

import traceback
import argparse
from pathlib import Path
import os.path
import os

parser = argparse.ArgumentParser(
                    prog = 'diag_budgets',
                    description = 'Diagnose daily budget',
)

parser.add_argument('--data-dir', type=str, help='Input data dir.', required=True)
parser.add_argument('--grid-dir', type=str, help='Input grid dir.', required=True)
parser.add_argument('--iters', type=int, help='The iteration.', required=True)
parser.add_argument('--nlev', type=int, help='The used vertical levels.', default=-1)
parser.add_argument('--output', type=str, help='Output filename', default="")
args = parser.parse_args()
print(args)


print("Load coordinate")
coo, crop_kwargs = lf.loadCoordinateFromFolderAndWithRange(args.grid_dir)
lat = coo.grid["YC"][:, 0]
lon = coo.grid["XC"][0, :]

z_T = coo.grid["RC"].flatten()
z_W = coo.grid["RF"].flatten()
mask = coo.grid["maskInC"]


bundle = mds.rdmds("%s/%s" % (args.data_dir, "diag_2D",), args.iters, returnmeta=True)
data = lf.postprocessRdmds(bundle)

print(data["EXFswnet"].shape)

my_EXFqnet = data["EXFswnet"] + data["EXFlwnet"] - data["EXFhs"] - data["EXFhl"]

print("Mean deviation of oceQnet and EXFqnet: ", np.mean(np.abs(data["oceQnet"] + data["EXFqnet"])))
print("Mean deviation of my_EXFqnet and EXFqnet: ", np.mean(np.abs(my_EXFqnet - data["EXFqnet"])))
print("Mean deviation of oceQsw and EXFswnet: ", np.mean(np.abs(data["oceQsw"] + data["EXFswnet"])))

import matplotlib as mplt
mplt.use("TkAgg")
import matplotlib.pyplot as plt


fig, ax = plt.subplots(2, 3, figsize = (12, 4))

levs = np.linspace(-1, 1, 51) * 200.0
levs2 = np.linspace(-1, 1, 51) * 1e-3

ax[0, 0].contourf(lon, lat, - data["oceQnet"], levels=levs, cmap="bwr")
ax[0, 1].contourf(lon, lat, data["EXFqnet"], levels=levs, cmap="bwr")
ax[0, 2].contourf(lon, lat, data["oceQnet"] + data["EXFqnet"], levels=levs2,  cmap="bwr")

ax[1, 0].contourf(lon, lat, my_EXFqnet, levels=levs, cmap="bwr")
mappable1 = ax[1, 1].contourf(lon, lat, data["EXFqnet"], levels=levs, cmap="bwr")
mappable2 = ax[1, 2].contourf(lon, lat, (my_EXFqnet - data["EXFqnet"]), levels=levs2,  cmap="bwr",)


plt.colorbar(mappable1, ax=ax[:, 0:2])
plt.colorbar(mappable2, ax=ax[:, 2])


ax[0, 0].set_title("- oceQnet")
ax[0, 1].set_title("EXFqnet")
ax[0, 2].set_title("EXFqnet + oceQnet")
ax[1, 0].set_title("my_EXFqnet")
ax[1, 1].set_title("EXFqnet")
ax[1, 2].set_title("my_EXFqnet - EXFqnet")

plt.show()


