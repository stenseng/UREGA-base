CREATE TABLE IF NOT EXISTS gnss_observations (
  obs_id SERIAL,
  rtcm_package_id  INTEGER,
  obs_epoch TIMESTAMP WITHOUT TIME ZONE NOT NULL,
  rtcm_msg_type SMALLINT NOT NULL,
  mountpoint varchar(50),
  sat_id char(4),
  sat_signal char(3),
  obs_code NUMERIC(10, 3),
  obs_phase NUMERIC(10, 3),
  obs_doppler NUMERIC(8, 4),
  obs_snr NUMERIC(6, 4)
);
SELECT create_hypertable('gnss_observations', 'obs_epoch', 'mountpoint', 2);
CREATE INDEX ON gnss_observations(mountpoint, sat_id, sat_signal, obs_epoch DESC);
CREATE INDEX ON gnss_observations(mountpoint, rtcm_msg_type, obs_epoch DESC);
