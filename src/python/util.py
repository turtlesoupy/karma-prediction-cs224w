import numpy as np

def xs(seq): return list(ixs(seq))
def ixs(seq): return (e[0] for e in seq)
def ys(seq): return list(iys(seq))
def iys(seq): return (e[1] for e in seq)

def mkccdf(ys):
    return [1 - e for e in mkcdf(ys)]

def mkcdf(ys):
    if ys == []:
        return []

    y = np.cumsum(ys)
    return [float(e) / y[-1] for e in y]


def mkpdf(ys):
    s = sum(ys)
    return [float(e) / s for e in ys]
