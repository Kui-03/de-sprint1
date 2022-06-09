__all__=['mkdir']

import os
from datetime import datetime

# Create Directory
def mkdir(_dir, force=False, format='%Y-%m-%d'): #_%H-%M-%S
    time = datetime.now().strftime(format)
    _dir = "%s/%s/" % (_dir, time)

    if not os.path.isdir(_dir) or force == True:
        os.makedirs(_dir)
        print("Created Directory at '{0}'".format(_dir))

    return _dir