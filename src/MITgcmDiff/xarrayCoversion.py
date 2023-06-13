import xarray as xr



def mkDataset(dict_dimarr, coo):
    
    data_vars = dict()
    for varname, _tmp in dict_dimarr.items():
    
        attrs = dict()

        if len(_tmp) == 2:
            dim, arr = _tmp
        elif len(_tmp) == 3:
            dim, arr, attrs = _tmp
        
        
        if dim == "T":
            dim_list = ["Z", "Y", "X"]
        elif dim == "sT":
            dim_list = ["Y", "X"]
        elif dim == "U":
            dim_list = ["Z", "Y", "X_l"]
        elif dim == "sU":
            dim_list = ["Y", "X_l"]
        elif dim == "V":
            dim_list = ["Z", "Y_l", "X"]
        elif dim == "sV":
            dim_list = ["Y_l", "X"]
        elif dim == "W":
            dim_list = ["Z_l", "Y", "X"]
        elif dim == "sW":
            dim_list = ["Y", "X"]
        
        else:
            raise Exception("Unknown dimension: %s" % (dim,))

        data_vars[varname] = (
            dim_list, arr, attrs,
        )
    
    ds = xr.Dataset(
        data_vars = data_vars,
        coords = dict(
            x = (["Y", "X"], coo.grid["XC"]),
            y = (["Y", "X"], coo.grid["YC"]),
            z = (["Z"], coo.grid["RC"][:, 0, 0]),
            x_l = (["Y", "X_l"], coo.grid["XG"]),
            y_l = (["Y_l", "X"], coo.grid["YG"]),
            z_l = (["Z_l"],      coo.grid["RF"][:-1, 0, 0]),
        ),
    )

    return ds
