from pylab import *

with open('errs.csv') as f:
    data = f.readlines()

pdata = []
for d in data:
    d = d.strip('\n').split(',')
    pdata.append(int(d[0]))
    pdata.append(float(d[1]))
    
print data
print pdata
x = pdata[::2]
y = pdata[1::2]
plt.plot(x, y)
# plt.axis([min(x), max(x), min(y), max(y)])
# show()
plt.savefig('plot.png')

