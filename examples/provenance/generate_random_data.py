## Chris's fabulous random data generator
############################################################
import random
data = [random.gauss(mu=0.0, sigma=1.0) for i in range(100)]
print ", ".join((str(n) for n in data))
