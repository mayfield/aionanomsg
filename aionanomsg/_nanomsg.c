/*
 * Low level bindings to nanomsg.
 */

#include "Python.h"
#include "structmember.h"
#include <errno.h>
#include <nanomsg/nn.h>
#include <stdio.h>


typedef struct {
    PyObject_HEAD
    int s;
} NNSocket;


static int NNSocket_init(NNSocket *self, PyObject *args, PyObject *kwds) {
    int domain = AF_SP;
    int protocol;
    int s;

    if (!PyArg_ParseTuple(args, "ii:NNSocket", &domain, &protocol))
        return -1;
    Py_BEGIN_ALLOW_THREADS
    s = nn_socket(domain, protocol);
    Py_END_ALLOW_THREADS
    if (s == -1) {
        PyErr_Format(PyExc_OSError, "nn_socket error: %s", nn_strerror(errno));
        return -1;
    }
    self->s = s;
    return 0;
}

static PyObject *NNSocket__nn_bind(NNSocket *self, PyObject *args) {
    const char *addr;
    int eid;

    if (!PyArg_ParseTuple(args, "s:_nn_bind", &addr))
        return NULL;
    Py_BEGIN_ALLOW_THREADS
    eid = nn_bind(self->s, addr);
    Py_END_ALLOW_THREADS
    if (eid == -1)
        return PyErr_Format(PyExc_OSError, "nn_bind error: %s",
                            nn_strerror(errno));
    return PyLong_FromLong(eid);
}

static PyObject * NNSocket__nn_connect(NNSocket *self, PyObject *args) {
    const char *addr;
    int eid;

    if (!PyArg_ParseTuple(args, "s:_nn_connect", &addr))
        return NULL;
    Py_BEGIN_ALLOW_THREADS
    eid = nn_connect(self->s, addr);
    Py_END_ALLOW_THREADS
    if (eid == -1)
        return PyErr_Format(PyExc_OSError, "nn_connect error: %s",
                            nn_strerror(errno));
    return PyLong_FromLong(eid);
}

static PyObject * NNSocket__nn_send(NNSocket *self, PyObject *args) {
    Py_buffer data;
    int flags;
    int nbytes;

    if (!PyArg_ParseTuple(args, "y*i:_nn_send", &data, &flags))
        return NULL;
    /* Handle EINTR retry internally */
    while (1) {
        Py_BEGIN_ALLOW_THREADS
        nbytes = nn_send(self->s, data.buf, data.len, flags);
        Py_END_ALLOW_THREADS
        if (nbytes != -1 || errno != EINTR || PyErr_CheckSignals())
            break;
        return NULL;
    }
    PyBuffer_Release(&data);
    if (nbytes == -1) {
        if (errno == EAGAIN) {
            PyErr_SetNone(PyExc_BlockingIOError);
        } else if (!PyErr_Occurred()) {
            PyErr_Format(PyExc_OSError, "nn_send error: %s",
                         nn_strerror(errno));
        }
        return NULL;
    }
    return PyLong_FromLong(nbytes);
}

static PyObject * NNSocket__nn_recv(NNSocket *self, PyObject *args) {
    int flags;
    char *buf;
    PyObject *data;
    int nbytes;

    if (!PyArg_ParseTuple(args, "i:_nn_recv", &flags))
        return NULL;
    /* Handle EINTR retry internally */
    while (1) {
        Py_BEGIN_ALLOW_THREADS
        nbytes = nn_recv(self->s, &buf, NN_MSG, flags);
        Py_END_ALLOW_THREADS
        if (nbytes != -1 || errno != EINTR || PyErr_CheckSignals())
            break;
        PyErr_SetNone(PyExc_ValueError);
        return NULL;
    }
    if (nbytes == -1) {
        if (errno == EAGAIN) {
            PyErr_SetNone(PyExc_BlockingIOError);
        } else if (!PyErr_Occurred()) {
            PyErr_Format(PyExc_OSError, "nn_recv error: %s",
                         nn_strerror(errno));
        }
        return NULL;
    }
    data = PyBytes_FromStringAndSize(buf, nbytes);
    nn_freemsg(buf);
    return data;
}

static PyObject * NNSocket__nn_getsockopt(NNSocket *self, PyObject *args) {
    int intval;
    char strval[4096];
    void *optval;
    size_t optsize;
    size_t size;
    int level;
    int option;
    int type;
    int r;

    if (!PyArg_ParseTuple(args, "iii:_nn_getsockopt", &level, &option, &type))
        return NULL;
    if (type == NN_TYPE_INT) {
        optval = &intval;
        optsize = (size = sizeof(intval));
    } else if (type == NN_TYPE_STR) {
        optval = &strval;
        optsize = (size = sizeof(strval));
    } else if (type == NN_TYPE_NONE) {
        optval = NULL;
        optsize = (size = 0);
    } else {
        return PyErr_Format(PyExc_TypeError, "invalid symbol type: %d", type);
    }
    Py_BEGIN_ALLOW_THREADS
    r = nn_getsockopt(self->s, level, option, optval, &optsize);
    Py_END_ALLOW_THREADS
    if (r == -1)
        return PyErr_Format(PyExc_OSError, "nn_getsockopt error: %s",
                            nn_strerror(errno));
    if (optsize > size)
        return PyErr_Format(PyExc_ValueError, "internal overflow");
    if (type == NN_TYPE_INT) {
        return PyLong_FromLong(intval);
    } else if (type == NN_TYPE_STR) {
        return PyUnicode_FromStringAndSize(strval, optsize);
    } else {
        Py_RETURN_NONE;
    }
}

static PyObject * NNSocket__nn_setsockopt(NNSocket *self, PyObject *args) {
    int intval;
    char *strptr;
    void *optval;
    size_t optsize;
    int level;
    int option;
    int type;
    int r;
    PyObject *value;

    if (!PyArg_ParseTuple(args, "iiiO:_nn_setsockopt", &level, &option, &type,
                          &value))
        return NULL;
    if (type == NN_TYPE_INT) {
        intval = PyLong_AsLong(value);
        if (PyErr_Occurred()) {
            return NULL;
        }
        optval = &intval;
        optsize = sizeof(intval);
    } else if (type == NN_TYPE_STR) {
        if ((strptr = PyBytes_AsString(value)) == NULL)
            return NULL;
        optval = strptr;
        optsize = PyBytes_Size(value);
    } else if (type == NN_TYPE_NONE) {
        optval = NULL;
        optsize = 0;
    } else {
        return PyErr_Format(PyExc_TypeError, "invalid symbol type: %d", type);
    }
    Py_BEGIN_ALLOW_THREADS
    r = nn_setsockopt(self->s, level, option, optval, optsize);
    Py_END_ALLOW_THREADS
    if (r == -1)
        return PyErr_Format(PyExc_OSError, "nn_setsockopt error: %s",
                            nn_strerror(errno));
    Py_RETURN_NONE;
}

static PyObject * NNSocket__nn_shutdown(NNSocket *self, PyObject *args) {
    int eid;

    if (!PyArg_ParseTuple(args, "i:_nn_shutdown", &eid))
        return NULL;
    Py_BEGIN_ALLOW_THREADS
    eid = nn_shutdown(self->s, eid);
    Py_END_ALLOW_THREADS
    if (eid == -1)
        return PyErr_Format(PyExc_OSError, "nn_shutdown error: %s",
                            nn_strerror(errno));
    Py_RETURN_NONE;
}

static PyObject * NNSocket__nn_get_statistic(NNSocket *self, PyObject *args) {
    return PyErr_Format(PyExc_NotImplementedError, "");
}

static PyObject * NNSocket__nn_device(NNSocket *self, PyObject *args) {
    return PyErr_Format(PyExc_NotImplementedError, "");
}


static PyMethodDef NNSocket_methods[] = {
    {"_nn_bind", (PyCFunction)NNSocket__nn_bind, METH_VARARGS, NULL},
    {"_nn_connect", (PyCFunction)NNSocket__nn_connect, METH_VARARGS, NULL},
    {"_nn_send", (PyCFunction)NNSocket__nn_send, METH_VARARGS, NULL},
    {"_nn_recv", (PyCFunction)NNSocket__nn_recv, METH_VARARGS, NULL},
    {"_nn_getsockopt", (PyCFunction)NNSocket__nn_getsockopt, METH_VARARGS,
     NULL},
    {"_nn_setsockopt", (PyCFunction)NNSocket__nn_setsockopt, METH_VARARGS,
     NULL},
    {"_nn_shutdown", (PyCFunction)NNSocket__nn_shutdown, METH_VARARGS, NULL},
    {"_nn_get_statistic", (PyCFunction)NNSocket__nn_get_statistic,
     METH_VARARGS, NULL},
    {"_nn_device", (PyCFunction)NNSocket__nn_device, METH_VARARGS, NULL},
    {NULL}
};

static PyMemberDef NNSocket_members[] = {
    {"_fd", T_INT, offsetof(NNSocket, s), 0, "Socket FD"},
    {NULL}
};

static PyTypeObject NNSocketType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "_nanomsg.NNSocket",
    .tp_basicsize = sizeof(NNSocket),
    .tp_flags = Py_TPFLAGS_DEFAULT|Py_TPFLAGS_BASETYPE,
    .tp_methods = NNSocket_methods,
    .tp_members = NNSocket_members,
    .tp_init = (initproc) NNSocket_init,
};


/**************************
 * Module definition/init *
 **************************/

static PyObject *get_symbol_info(PyObject *mod, PyObject *_na) {
    int i;
    PyObject *dict = PyDict_New();
    for (i = 0;; ++i) {
        struct nn_symbol_properties sym;
        PyObject *val;
        if (!nn_symbol_info(i, &sym, sizeof(sym)))
            break;
        val = Py_BuildValue("{s:i, s:i, s:i, s:i}", 
                            "value", sym.value,
                            "ns", sym.ns,
                            "type", sym.type,
                            "unit", sym.unit);
        if (val == NULL) {
            Py_DECREF(dict);
            return NULL;
        }
        if (PyDict_SetItemString(dict, sym.name, val) == -1) {
            return NULL;
        }
    }
    return dict;
}

static PyMethodDef _nanomsg_methods[] = {
    {"get_symbol_info",  get_symbol_info, METH_NOARGS, NULL},
    {NULL, NULL, 0, NULL}        /* Sentinel */
};

static struct PyModuleDef _nanomsg_module = {
    PyModuleDef_HEAD_INIT,
    "cnanomsg._nanomsg",
    "C bindings for nanomsg",
    -1,
    _nanomsg_methods,
};


PyMODINIT_FUNC PyInit__nanomsg(void) {
    PyObject* m;
    int i, value;
    const char *symbol;

    NNSocketType.tp_new = PyType_GenericNew;
    if (PyType_Ready(&NNSocketType) < 0)
        return NULL;
    m = PyModule_Create(&_nanomsg_module);
    if (m == NULL)
        return NULL;
    Py_INCREF(&NNSocketType);
    PyModule_AddObject(m, "NNSocket", (PyObject *)&NNSocketType);
    /* Add constants */
    for (i = 0; ; ++i) {
        symbol = nn_symbol(i, &value);
        if (symbol == NULL)
            break;
        PyModule_AddIntConstant(m, symbol, value);
    }

    return m;
}
