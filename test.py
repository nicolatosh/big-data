import numpy as np

x = np.arange(8,20,0.1)
y = np.array([0.0 for num in x])

for i in range(len(x)):
    y[i] = np.sin(x[i]-2) + 2

#I multiply each y[i] for 10 to simulate a number of transactions
y = [int(np.floor(10*el)) for el in y] #number of transactions every 0.1 h (6 min)
print(y)   
print(x)
