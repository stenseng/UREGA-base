CREATE TABLE IF NOT EXISTS gps_observations (
    obs_id SERIAL,
    rtcm_package_id  INTEGER,
    obs_epoch TIMESTAMP WITH TIME ZONE NOT NULL,
    rtcm_msg_type SMALLINT NOT NULL,
    mountpoint VARCHAR(50),
    sat_id CHAR(4),
    sat_signal CHAR(3),
    obs_code NUMERIC(13, 10),
    obs_phase NUMERIC(14, 11),
    obs_doppler NUMERIC(8, 4),
    obs_snr NUMERIC(6, 4),
    obs_lock_time_indicator INTEGER
);

SELECT create_hypertable('gps_observations', 'obs_epoch', 'mountpoint', 2);
CREATE INDEX ON gps_observations(mountpoint, sat_id, sat_signal, obs_epoch DESC);
CREATE INDEX ON gps_observations(mountpoint, rtcm_msg_type, obs_epoch DESC);
