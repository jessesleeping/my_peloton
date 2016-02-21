if [ "$#" -ne 1 ]; then
    OPT=""
else
    OPT="-i $1"
fi

scp $OPT -P 2222 src/backend/index/bwtree_index.cpp vagrant@127.0.0.1:/home/vagrant/peloton/src/backend/index/
scp $OPT -P 2222 src/backend/index/bwtree_index.h vagrant@127.0.0.1:/home/vagrant/peloton/src/backend/index/
scp $OPT -P 2222 src/backend/index/bwtree.cpp vagrant@127.0.0.1:/home/vagrant/peloton/src/backend/index/
scp $OPT -P 2222 src/backend/index/bwtree.h vagrant@127.0.0.1:/home/vagrant/peloton/src/backend/index/
scp $OPT -P 2222 src/backend/index/Makefile.am vagrant@127.0.0.1:/home/vagrant/peloton/src/backend/index/
scp $OPT -P 2222 tests/index/index_test.cpp vagrant@127.0.0.1:/home/vagrant/peloton/tests/index/
