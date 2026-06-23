limitless_probes*:::read-start { 
    self->read_ts = timestamp; 
}

limitless_probes*:::read-done /self->read_ts/ {
    @read_lat = quantize(timestamp - self->read_ts);
    self->read_ts = 0;
}

limitless_probes*:::write-start { 
    self->write_ts = timestamp; 
}

limitless_probes*:::write-done /self->write_ts/ {
    @write_lat = quantize(timestamp - self->write_ts);
    self->write_ts = 0;
}
