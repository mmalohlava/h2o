from pylab import *

with open('errs.csv') as f:
    data = f.readlines()

pdata = []
for d in data:
    d = d.strip('\n').split(',')
    pdata.append(int(d[0]))
    pdata.append(100-float(d[1]))
    pdata.append(float(d[2]))

print pdata    
x = pdata[::3]
y = pdata[1::3]
z = pdata[2::3]
print x
print y
print z
plt.plot(x, y,'.-')
plt.xlabel('# of trees')
plt.ylabel('% correct')
plt.axis([0, max(x)+5, min(y)-.2, max(y)+.1])
# show()
plt.savefig('err_plot.png')
plt.clf()
plt.plot(x,z,'.-')
plt.ylabel('Time to train (s)')
plt.xlabel('# of trees')
plt.savefig('t_plot.png')
