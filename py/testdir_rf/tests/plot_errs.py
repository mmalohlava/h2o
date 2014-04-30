import sys
from pylab import *

TREES=0
SAMPLES=1
MTRY=2
ERR=3
TIME=4
COLS=5
NODES=1
ERR_D=2
TIME_D=3
COLS_D=4

if sys.argv[1] == 'samples':
    with open('s_log.csv') as f:
        data = f.readlines()
    pdata = []
    for d in data:
        d = d.strip('\n').split(',')
        pdata.append(int(d[TREES]))
        pdata.append(int(d[SAMPLES]))
        pdata.append(int(d[MTRY]))
        pdata.append(100-float(d[ERR]))
        pdata.append(float(d[TIME]))

    x = pdata[SAMPLES::COLS]
    y = pdata[ERR::COLS]
    z = pdata[TIME::COLS]
    print x
    print y
    print z

elif sys.argv[1] == 'mtry':
    with open('m_log.csv') as f:
        data = f.readlines()
    pdata = []
    for d in data:
        d = d.strip('\n').split(',')
        pdata.append(int(d[TREES]))
        pdata.append(int(d[SAMPLES]))
        pdata.append(int(d[MTRY]))
        pdata.append(100-float(d[ERR]))
        pdata.append(float(d[TIME]))
        
    x = pdata[MTRY::COLS]
    y = pdata[ERR::COLS]
    z = pdata[TIME::COLS]
    print x
    print y
    print z

elif sys.argv[1] == 'dist':
    with open('dist_log.csv') as f:
        data = f.readlines()
    pdata = []
    for d in data:
        d = d.strip('\n').split(',')
        pdata.append(int(d[TREES]))
        pdata.append(int(d[NODES]))
        pdata.append(100-float(d[ERR_D]))
        pdata.append(float(d[TIME_D]))
        
    x = pdata[NODES::COLS_D]
    y = pdata[ERR_D::COLS_D]
    z = pdata[TIME_D::COLS_D]

elif sys.argv[1] == 'par' or sys.argv[1] == 'divotes':
    if sys.argv[1] == 'par':
        with open('par_log.csv') as f:
            data = f.readlines()
    else:
        with open('divotes_log.csv') as f:
            data = f.readlines()
    pdata = []
    for d in data:
        d = d.strip('\n').split(',')
        pdata.append(int(d[TREES]))
        pdata.append(int(d[NODES]))
        pdata.append(100-float(d[ERR_D]))
        pdata.append(float(d[TIME_D]))
        
    x = pdata[NODES::COLS_D]
    y = pdata[ERR_D::COLS_D]
    z = pdata[TIME_D::COLS_D]

plt.plot(x,y,'.-')
if sys.argv[1] == 'samples':
    plt.xlabel('Samples (%)')
elif sys.argv[1] == 'mtry':
    plt.xlabel('mtry value')
elif sys.argv[1] == 'dist' or sys.argv[1] == 'par' or sys.argv[1] == 'divotes':
    plt.xlabel('Nodes')
    
plt.ylabel('% correct')
plt.axis([0, max(x)+5, min(y)-.2, max(y)+.1])

# show()
if sys.argv[1] == 'samples':
    plt.title('samples accuracy')
    plt.savefig('plot_s_e.png')
elif sys.argv[1] == 'mtry':
    plt.title('mtry accuracy')
    plt.savefig('plot_m_e.png')
elif sys.argv[1] == 'dist':
    plt.title('Distributed accuracy')
    plt.savefig('plot_n_e.png')
elif sys.argv[1] == 'divotes':
    plt.title('DIVotes accuracy')
    plt.savefig('plot_d_e.png')
elif sys.argv[1] == 'par':
    plt.title('Parallel accuracy')
    plt.savefig('plot_p_e.png')
plt.clf()

plt.plot(x,z,'.-')
plt.ylabel('Time to train (s)')
if sys.argv[1] == 'samples':
    plt.title('samples time')
    plt.xlabel('Samples (%)')
    plt.savefig('plot_s_t.png')
elif sys.argv[1] == 'mtry':
    plt.title('mtry time')
    plt.xlabel('mtry value')
    plt.savefig('plot_m_t.png')
elif sys.argv[1] == 'dist':
    plt.title('Distributed time')
    plt.xlabel('Nodes')
    plt.savefig('plot_n_t.png')
elif sys.argv[1] == 'divotes':
    plt.title('DIVotes time')
    plt.xlabel('Nodes')
    plt.savefig('plot_d_t.png')
elif sys.argv[1] == 'par':
    plt.title('Parallel time')
    plt.xlabel('Nodes')
    plt.savefig('plot_p_t.png')
