import numpy as np

class Coordinate:

    def __init__(self, grid):

        self.grid = grid

        self.Ny = grid["DXG"].shape[0]
        self.Nx = grid["DXG"].shape[1]
        self.Nz = grid["RC"].shape[0]

        #self.grid["DXG_p1"] = np.roll(self.grid["DXG"], -1, axis=0)
        #self.grid["DYG_p1"] = np.roll(self.grid["DYG"], -1, axis=1)

        #print("RF: ")
        #print(self.grid["RF"])
    
        self.grid["RAC_slab"] = self.grid["RAC"].reshape((1, self.Ny, self.Nx))
        self.grid["DVOLT"] = self.grid["DRF"] * self.grid["RAC_slab"]


        Nz_bot = np.zeros_like(self.grid["DXG"], dtype=int)
        mask = self.grid["maskInC"]
        Nz_bot_test = self.grid["RF"] < - np.expand_dims(self.grid["Depth"], 0)
        exists_Nz_bot = np.sum(Nz_bot_test, axis=0)
        for j in range(self.Ny):
            for i in range(self.Nx):
                if mask[j, i] == 0:
                    Nz_bot[j, i] = -1
                    continue

                if exists_Nz_bot[j, i] == 0:
                    Nz_bot[j, i] = self.Nz-1
                    continue
                
                        
                Nz_bot[j, i] = self.Nz-1
                for k in range(self.Nz):
                    if Nz_bot_test[k, j, i]:
                        Nz_bot[j, i] = k - 1
                        break

                #print("(%d, %d) : %d" % (j, i, Nz_bot[j, i]))
        if np.any((Nz_bot < 0) & (mask == 1)):
            raise Exception("Some Nz_bot is negative. Please check.")

        self.grid["Nz_bot"] = Nz_bot



