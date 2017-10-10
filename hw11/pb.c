#include <Python.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <math.h>
#include "deviceapps.pb-c.h"
#include "zlib.h"

#define MAGIC  0xFFFFFFFF
#define DEVICE_APPS_TYPE 1


typedef struct pbheader_s {
    uint32_t magic;
    uint16_t type;
    uint16_t length;
} pbheader_t;
#define PBHEADER_INIT {MAGIC, DEVICE_APPS_TYPE, 0}


typedef struct _Buffer {
    void* p;
    size_t len;
} Buffer;

#define BUFFER_INIT {NULL, 0}

int Buffer_Resize(Buffer* self, size_t len){
    if (self->len < len){
        void* p = malloc(len);
	if (p){
	    free(self->p);
	    self->p = p;
	    self->len = len;
	}else{
            return 0;
	}
    }
    return 1;
}

void Buffer_Destroy(Buffer* self){
    free(self->p);
    self->p = NULL;
    self->len = 0;
}

size_t serialize_dict(PyObject* dict, Buffer* out, Buffer* tmp_buf){
    DeviceApps msg = DEVICE_APPS__INIT;
    DeviceApps__Device device = DEVICE_APPS__DEVICE__INIT;

    // device
    PyObject* device_obj = PyDict_GetItemString(dict, "device");
    if (device_obj){
        if (! PyDict_Check(device_obj)){
            PyErr_SetString(PyExc_ValueError, "'device' must be a dictionary");
	    return 0;
	}
	// id
        PyObject* id_obj = PyDict_GetItemString(device_obj, "id");
	if (id_obj) {
            if (! PyString_Check(id_obj)){
                PyErr_SetString(PyExc_ValueError, "'id' must be a string");
		return 0;
	    }
	    device.has_id = 1;
	    device.id.len = PyString_Size(id_obj);
	    device.id.data = PyString_AsString(id_obj);
	}
	// type
        PyObject* type_obj = PyDict_GetItemString(device_obj, "type");
        if (type_obj) {
            if (! PyString_Check(type_obj)){
                    PyErr_SetString(PyExc_ValueError, "'type' must be a string");
                    return 0;
	    }
            device.has_type = 1;
            device.type.len = PyString_Size(type_obj);
            device.type.data = PyString_AsString(type_obj);
        }
    }
    msg.device = &device;
    
    // apps
    PyObject* apps_obj = PyDict_GetItemString(dict, "apps");
    if (apps_obj){
        if (! PyList_Check(apps_obj)){
            PyErr_SetString(PyExc_ValueError, "'apps' must be a list");
	    return 0;
	}
	msg.n_apps = PyList_Size(apps_obj);
	if (msg.n_apps){
            Buffer_Resize(tmp_buf, sizeof(uint32_t) * msg.n_apps);
	    msg.apps = tmp_buf->p;
	    size_t i=0;
	    while (i<msg.n_apps){
                PyObject* item = PyList_GetItem(apps_obj, i);
		if (! PyInt_Check(item)){
                    PyErr_SetString(PyExc_ValueError, "'apps' must contain only integers");
		    return 0;
		}
		msg.apps[i] = PyInt_AsLong(item);
		++i;
	    }
	}
    }

    // lat
    PyObject* lat_obj = PyDict_GetItemString(dict, "lat");
    if (lat_obj){
	if (PyFloat_Check(lat_obj)){
            msg.has_lat = 1;
            msg.lat = PyFloat_AsDouble(lat_obj);
	} else if (PyInt_Check(lat_obj)){
            msg.has_lat = 1;
	    msg.lat = PyInt_AsLong(lat_obj);
	} else {
            PyErr_SetString(PyExc_ValueError, "'lat' must be a float or int");
	    return 0;
	}
    }

    // lon
    PyObject* lon_obj = PyDict_GetItemString(dict, "lon");
    if (lon_obj){
        if (PyFloat_Check(lon_obj)){
            msg.has_lon = 1;
            msg.lon = PyFloat_AsDouble(lon_obj);
        } else if (PyInt_Check(lon_obj)){
            msg.has_lon = 1;
            msg.lon = PyInt_AsLong(lon_obj);
        } else {
            PyErr_SetString(PyExc_ValueError, "'lon' must be a float or int");
            return 0;
        }
    }

    size_t nbytes = device_apps__get_packed_size(&msg);
    Buffer_Resize(out, 8+nbytes);
    pbheader_t header = PBHEADER_INIT;
    header.length = nbytes;
    memcpy(out->p, &header.magic, 4);
    memcpy(out->p+4, &header.type, 2);
    memcpy(out->p+6, &header.length, 2);
    device_apps__pack(&msg, out->p+8);
    return 8+nbytes;
}

// Read iterator of Python dicts
// Pack them to DeviceApps protobuf and write to file with appropriate header
// Return number of written bytes as Python integer
static PyObject* py_deviceapps_xwrite_pb(PyObject* self, PyObject* args) {
    const char* path;
    PyObject* o;

    if (!PyArg_ParseTuple(args, "Os", &o, &path))
        return NULL;

    PyObject* iter = PyObject_GetIter(o);
    if (iter == NULL){
	PyErr_SetString(PyExc_TypeError, "The first argument must be an iterable object");
	return NULL;
    }
    gzFile fp = gzopen(path, "wb666666");
    if (fp == NULL){
        PyErr_SetString(PyExc_IOError, "Cannot open a file");
        Py_DECREF(iter);
	return NULL;
    }

    unsigned PY_LONG_LONG total_len = 0;
    PyObject* item;
    Buffer serialized = BUFFER_INIT;
    Buffer tmp_buf = BUFFER_INIT;
    while (item = PyIter_Next(iter)){
        if (! PyDict_Check(item)){
            PyErr_SetString(PyExc_ValueError, "The first argument must contain only dictionaries");
	    Py_DECREF(item);
	    gzclose(fp);
	    return NULL;
	}
	size_t len;
	if (! (len = serialize_dict(item, &serialized, &tmp_buf))){
	    Py_DECREF(item);
	    gzclose(fp);
	    return NULL;
	}
	gzwrite(fp, serialized.p, len);
	Py_DECREF(item);
	total_len += len;
    }
    Buffer_Destroy(&tmp_buf);
    Buffer_Destroy(&serialized);
    Py_DECREF(iter);
    gzclose(fp);

    return PyLong_FromUnsignedLongLong(total_len);
}

// Unpack only messages with type == DEVICE_APPS_TYPE
// Return iterator of Python dicts
static PyObject* py_deviceapps_xread_pb(PyObject* self, PyObject* args) {
    const char* path;

    if (!PyArg_ParseTuple(args, "s", &path))
        return NULL;

    printf("Read from: %s\n", path);
    Py_RETURN_NONE;
}


static PyMethodDef PBMethods[] = {
     {"deviceapps_xwrite_pb", py_deviceapps_xwrite_pb, METH_VARARGS, "Write serialized protobuf to file fro iterator"},
     {"deviceapps_xread_pb", py_deviceapps_xread_pb, METH_VARARGS, "Deserialize protobuf from file, return iterator"},
     {NULL, NULL, 0, NULL}
};


PyMODINIT_FUNC initpb(void) {
     (void) Py_InitModule("pb", PBMethods);
}
