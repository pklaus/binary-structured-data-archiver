BSD - binary structured data

### Possible Backends

#### numpy Structured arrays

<https://numpy.org/doc/stable/user/basics.rec.html>

#### btsf - binary time series format

<https://github.com/pklaus/btsf>

#### teafiles

<https://discretelogics.com/teafiles/>
<https://github.com/discretelogics/TeaFiles.Py>

#### Protocol Buffers

<https://developers.google.com/protocol-buffers>
Maybe using the .proto used by archiver appliance:
<https://github.com/slacmshankar/epicsarchiverap/blob/master/src/main/edu/stanford/slac/archiverappliance/PB/EPICSEvent.proto>

### Mapping CA types to Python types

caproto:
<https://github.com/caproto/caproto/blob/master/caproto/_numpy_backend.py#L33>

epics\_dash:
<https://github.com/pklaus/pytrbnet/blob/master/trbnet/epics/pcaspy_ioc.py#L149>
