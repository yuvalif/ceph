.. _health-checks:

===============
 Health checks 
===============

Overview
========

There is a finite set of health messages that a Ceph cluster can raise. These
messages are known as *health checks*. Each health check has a unique
identifier.

The identifier is a terse human-readable string -- that is, the identifier is
readable in much the same way as a typical variable name. It is intended to
enable tools (for example, UIs) to make sense of health checks and present them
in a way that reflects their meaning.

This page lists the health checks that are raised by the monitor and manager
daemons. In addition to these, you might see health checks that originate
from MDS daemons (see :ref:`cephfs-health-messages`), and health checks
that are defined by ``ceph-mgr`` python modules.

Definitions
===========

Monitor
-------

DAEMON_OLD_VERSION
__________________

Warn if one or more old versions of Ceph are running on any daemons.  A health
check is raised if multiple versions are detected.  This condition must exist
for a period of time greater than ``mon_warn_older_version_delay`` (set to one
week by default) in order for the health check to be raised. This allows most
upgrades to proceed without the occurrence of a false warning. If the upgrade
is paused for an extended time period, ``health mute`` can be used by running
``ceph health mute DAEMON_OLD_VERSION --sticky``. Be sure, however, to run
``ceph health unmute DAEMON_OLD_VERSION`` after the upgrade has finished.

MON_DOWN
________

One or more monitor daemons are currently down. The cluster requires a majority
(more than one-half) of the monitors to be available. When one or more monitors
are down, clients might have a harder time forming their initial connection to
the cluster, as they might need to try more addresses before they reach an
operating monitor.

The down monitor daemon should be restarted as soon as possible to reduce the
risk of a subsequent monitor failure leading to a service outage.

MON_CLOCK_SKEW
______________

The clocks on the hosts running the ceph-mon monitor daemons are not
well-synchronized. This health check is raised if the cluster detects a clock
skew greater than ``mon_clock_drift_allowed``.

This issue is best resolved by synchronizing the clocks by using a tool like
``ntpd`` or ``chrony``.

If it is impractical to keep the clocks closely synchronized, the
``mon_clock_drift_allowed`` threshold can also be increased. However, this
value must stay significantly below the ``mon_lease`` interval in order for the
monitor cluster to function properly.

MON_MSGR2_NOT_ENABLED
_____________________

The :confval:`ms_bind_msgr2` option is enabled but one or more monitors are
not configured to bind to a v2 port in the cluster's monmap. This
means that features specific to the msgr2 protocol (for example, encryption)
are unavailable on some or all connections.

In most cases this can be corrected by running the following command:

.. prompt:: bash $

   ceph mon enable-msgr2

After this command is run, any monitor configured to listen on the old default
port (6789) will continue to listen for v1 connections on 6789 and begin to
listen for v2 connections on the new default port 3300.

If a monitor is configured to listen for v1 connections on a non-standard port
(that is, a port other than 6789), then the monmap will need to be modified
manually.


MON_DISK_LOW
____________

One or more monitors are low on disk space. This health check is raised if the
percentage of available space on the file system used by the monitor database
(normally ``/var/lib/ceph/mon``) drops below the percentage value
``mon_data_avail_warn`` (default: 30%).

This alert might indicate that some other process or user on the system is
filling up the file system used by the monitor. It might also
indicate that the monitor database is too large (see ``MON_DISK_BIG``
below).

If space cannot be freed, the monitor's data directory might need to be
moved to another storage device or file system (this relocation process must be carried out while the monitor
daemon is not running).


MON_DISK_CRIT
_____________

One or more monitors are critically low on disk space. This health check is raised if the
percentage of available space on the file system used by the monitor database
(normally ``/var/lib/ceph/mon``) drops below the percentage value
``mon_data_avail_crit`` (default: 5%). See ``MON_DISK_LOW``, above.

MON_DISK_BIG
____________

The database size for one or more monitors is very large. This health check is
raised if the size of the monitor database is larger than
``mon_data_size_warn`` (default: 15 GiB).

A large database is unusual, but does not necessarily indicate a problem.
Monitor databases might grow in size when there are placement groups that have
not reached an ``active+clean`` state in a long time.

This alert might also indicate that the monitor's database is not properly
compacting, an issue that has been observed with some older versions of leveldb
and rocksdb. Forcing a compaction with ``ceph daemon mon.<id> compact`` might
shrink the database's on-disk size.

This alert might also indicate that the monitor has a bug that prevents it from
pruning the cluster metadata that it stores. If the problem persists, please
report a bug.

To adjust the warning threshold, run the following command:

.. prompt:: bash $

   ceph config set global mon_data_size_warn <size>


AUTH_INSECURE_GLOBAL_ID_RECLAIM
_______________________________

One or more clients or daemons that are connected to the cluster are not
securely reclaiming their ``global_id`` (a unique number that identifies each
entity in the cluster) when reconnecting to a monitor. The client is being
permitted to connect anyway because the
``auth_allow_insecure_global_id_reclaim`` option is set to ``true`` (which may
be necessary until all Ceph clients have been upgraded) and because the
``auth_expose_insecure_global_id_reclaim`` option is set to ``true`` (which
allows monitors to detect clients with "insecure reclaim" sooner by forcing
those clients to reconnect immediately after their initial authentication).

To identify which client(s) are using unpatched Ceph client code, run the
following command:

.. prompt:: bash $

   ceph health detail

If you collect a dump of the clients that are connected to an individual
monitor and examine the ``global_id_status`` field in the output of the dump,
you can see the ``global_id`` reclaim behavior of those clients. Here
``reclaim_insecure`` means that a client is unpatched and is contributing to
this health check.  To effect a client dump, run the following command:

.. prompt:: bash $

   ceph tell mon.\* sessions

We strongly recommend that all clients in the system be upgraded to a newer
version of Ceph that correctly reclaims ``global_id`` values. After all clients
have been updated, run the following command to stop allowing insecure
reconnections:

.. prompt:: bash $

   ceph config set mon auth_allow_insecure_global_id_reclaim false

If it is impractical to upgrade all clients immediately, you can temporarily
silence this alert by running the following command:

.. prompt:: bash $

   ceph health mute AUTH_INSECURE_GLOBAL_ID_RECLAIM 1w   # 1 week

Although we do NOT recommend doing so, you can also disable this alert
indefinitely by running the following command:

.. prompt:: bash $

   ceph config set mon mon_warn_on_insecure_global_id_reclaim false

AUTH_INSECURE_GLOBAL_ID_RECLAIM_ALLOWED
_______________________________________

Ceph is currently configured to allow clients that reconnect to monitors using
an insecure process to reclaim their previous ``global_id``. Such reclaiming is
allowed because, by default, ``auth_allow_insecure_global_id_reclaim`` is set
to ``true``. It might be necessary to leave this setting enabled while existing
Ceph clients are upgraded to newer versions of Ceph that correctly and securely
reclaim their ``global_id``.

If the ``AUTH_INSECURE_GLOBAL_ID_RECLAIM`` health check has not also been
raised and if the ``auth_expose_insecure_global_id_reclaim`` setting has not
been disabled (it is enabled by default), then there are currently no clients
connected that need to be upgraded. In that case, it is safe to disable
``insecure global_id reclaim`` by running the following command:

.. prompt:: bash $

   ceph config set mon auth_allow_insecure_global_id_reclaim false

On the other hand, if there are still clients that need to be upgraded, then
this alert can be temporarily silenced by running the following command:

.. prompt:: bash $

   ceph health mute AUTH_INSECURE_GLOBAL_ID_RECLAIM_ALLOWED 1w   # 1 week

Although we do NOT recommend doing so, you can also disable this alert indefinitely
by running the following command:

.. prompt:: bash $

   ceph config set mon mon_warn_on_insecure_global_id_reclaim_allowed false


Manager
-------

MGR_DOWN
________

All manager daemons are currently down. The cluster should normally have at
least one running manager (``ceph-mgr``) daemon. If no manager daemon is
running, the cluster's ability to monitor itself will be compromised, and parts
of the management API will become unavailable (for example, the dashboard will
not work, and most CLI commands that report metrics or runtime state will
block). However, the cluster will still be able to perform all I/O operations
and to recover from failures.

The "down" manager daemon should be restarted as soon as possible to ensure
that the cluster can be monitored (for example, so that the ``ceph -s``
information is up to date, or so that metrics can be scraped by Prometheus).


MGR_MODULE_DEPENDENCY
_____________________

An enabled manager module is failing its dependency check. This health check
typically comes with an explanatory message from the module about the problem.

For example, a module might report that a required package is not installed: in
this case, you should install the required package and restart your manager
daemons.

This health check is applied only to enabled modules. If a module is not
enabled, you can see whether it is reporting dependency issues in the output of
`ceph module ls`.


MGR_MODULE_ERROR
________________

A manager module has experienced an unexpected error. Typically, this means
that an unhandled exception was raised from the module's `serve` function. The
human-readable description of the error might be obscurely worded if the
exception did not provide a useful description of itself.

This health check might indicate a bug: please open a Ceph bug report if you
think you have encountered a bug.

However, if you believe the error is transient, you may restart your manager
daemon(s) or use ``ceph mgr fail`` on the active daemon in order to force
failover to another daemon.

OSDs
----

OSD_DOWN
________

One or more OSDs are marked "down". The ceph-osd daemon might have been
stopped, or peer OSDs might be unable to reach the OSD over the network.
Common causes include a stopped or crashed daemon, a "down" host, or a network
outage.

Verify that the host is healthy, the daemon is started, and the network is
functioning. If the daemon has crashed, the daemon log file
(``/var/log/ceph/ceph-osd.*``) might contain debugging information.

OSD_<crush type>_DOWN
_____________________

(for example, OSD_HOST_DOWN, OSD_ROOT_DOWN)

All of the OSDs within a particular CRUSH subtree are marked "down" (for
example, all OSDs on a host).

OSD_ORPHAN
__________

An OSD is referenced in the CRUSH map hierarchy, but does not exist.

To remove the OSD from the CRUSH map hierarchy, run the following command:

.. prompt:: bash $

   ceph osd crush rm osd.<id>

OSD_OUT_OF_ORDER_FULL
_____________________

The utilization thresholds for `nearfull`, `backfillfull`, `full`, and/or
`failsafe_full` are not ascending. In particular, the following pattern is
expected: `nearfull < backfillfull`, `backfillfull < full`, and `full <
failsafe_full`.

To adjust these utilization thresholds, run the following commands:

.. prompt:: bash $

   ceph osd set-nearfull-ratio <ratio>
   ceph osd set-backfillfull-ratio <ratio>
   ceph osd set-full-ratio <ratio>


OSD_FULL
________

One or more OSDs have exceeded the `full` threshold and are preventing the
cluster from servicing writes.

To check utilization by pool, run the following command:

.. prompt:: bash $

   ceph df

To see the currently defined `full` ratio, run the following command:

.. prompt:: bash $

   ceph osd dump | grep full_ratio

A short-term workaround to restore write availability is to raise the full
threshold by a small amount. To do so, run the following command:

.. prompt:: bash $

   ceph osd set-full-ratio <ratio>

Additional OSDs should be deployed in order to add new storage to the cluster,
or existing data should be deleted in order to free up space in the cluster.

OSD_BACKFILLFULL
________________

One or more OSDs have exceeded the `backfillfull` threshold or *would* exceed
it if the currently-mapped backfills were to finish, which will prevent data
from rebalancing to this OSD. This alert is an early warning that
rebalancing might be unable to complete and that the cluster is approaching
full.

To check utilization by pool, run the following command:

.. prompt:: bash $

   ceph df

OSD_NEARFULL
____________

One or more OSDs have exceeded the `nearfull` threshold. This alert is an early
warning that the cluster is approaching full.

To check utilization by pool, run the following command:

.. prompt:: bash $

   ceph df

OSDMAP_FLAGS
____________

One or more cluster flags of interest have been set. These flags include:

* *full* - the cluster is flagged as full and cannot serve writes
* *pauserd*, *pausewr* - there are paused reads or writes
* *noup* - OSDs are not allowed to start
* *nodown* - OSD failure reports are being ignored, and that means that the
  monitors will not mark OSDs "down"
* *noin* - OSDs that were previously marked ``out`` are not being marked
  back ``in`` when they start
* *noout* - "down" OSDs are not automatically being marked ``out`` after the
  configured interval
* *nobackfill*, *norecover*, *norebalance* - recovery or data
  rebalancing is suspended
* *noscrub*, *nodeep_scrub* - scrubbing is disabled
* *notieragent* - cache-tiering activity is suspended

With the exception of *full*, these flags can be set or cleared by running the
following commands:

.. prompt:: bash $

   ceph osd set <flag>
   ceph osd unset <flag>

OSD_FLAGS
_________

One or more OSDs or CRUSH {nodes,device classes} have a flag of interest set.
These flags include:

* *noup*: these OSDs are not allowed to start
* *nodown*: failure reports for these OSDs will be ignored
* *noin*: if these OSDs were previously marked ``out`` automatically
  after a failure, they will not be marked ``in`` when they start
* *noout*: if these OSDs are "down" they will not automatically be marked
  ``out`` after the configured interval

To set and clear these flags in batch, run the following commands:

.. prompt:: bash $

   ceph osd set-group <flags> <who>
   ceph osd unset-group <flags> <who>

For example:

.. prompt:: bash $

   ceph osd set-group noup,noout osd.0 osd.1
   ceph osd unset-group noup,noout osd.0 osd.1
   ceph osd set-group noup,noout host-foo
   ceph osd unset-group noup,noout host-foo
   ceph osd set-group noup,noout class-hdd
   ceph osd unset-group noup,noout class-hdd

OLD_CRUSH_TUNABLES
__________________

The CRUSH map is using very old settings and should be updated. The oldest set
of tunables that can be used (that is, the oldest client version that can
connect to the cluster) without raising this health check is determined by the
``mon_crush_min_required_version`` config option.  For more information, see
:ref:`crush-map-tunables`.

OLD_CRUSH_STRAW_CALC_VERSION
____________________________

The CRUSH map is using an older, non-optimal method of calculating intermediate
weight values for ``straw`` buckets.

The CRUSH map should be updated to use the newer method (that is:
``straw_calc_version=1``). For more information, see :ref:`crush-map-tunables`.

CACHE_POOL_NO_HIT_SET
_____________________

One or more cache pools are not configured with a *hit set* to track
utilization. This issue prevents the tiering agent from identifying cold
objects that are to be flushed and evicted from the cache.

To configure hit sets on the cache pool, run the following commands:

.. prompt:: bash $

   ceph osd pool set <poolname> hit_set_type <type>
   ceph osd pool set <poolname> hit_set_period <period-in-seconds>
   ceph osd pool set <poolname> hit_set_count <number-of-hitsets>
   ceph osd pool set <poolname> hit_set_fpp <target-false-positive-rate>

OSD_NO_SORTBITWISE
__________________

No pre-Luminous v12.y.z OSDs are running, but the ``sortbitwise`` flag has not
been set.

The ``sortbitwise`` flag must be set in order for OSDs running Luminous v12.y.z
or newer to start. To safely set the flag, run the following command:

.. prompt:: bash $

   ceph osd set sortbitwise

OSD_FILESTORE
__________________

Warn if OSDs are running Filestore. The Filestore OSD back end has been
deprecated; the BlueStore back end has been the default object store since the
Ceph Luminous release.

The 'mclock_scheduler' is not supported for Filestore OSDs. For this reason,
the default 'osd_op_queue' is set to 'wpq' for Filestore OSDs and is enforced
even if the user attempts to change it.



.. prompt:: bash $

   ceph report | jq -c '."osd_metadata" | .[] | select(.osd_objectstore | contains("filestore")) | {id, osd_objectstore}'

**In order to upgrade to Reef or a later release, you must first migrate any
Filestore OSDs to BlueStore.**

If you are upgrading a pre-Reef release to Reef or later, but it is not
feasible to migrate Filestore OSDs to BlueStore immediately, you can
temporarily silence this alert by running the following command:

.. prompt:: bash $

   ceph health mute OSD_FILESTORE

Since this migration can take a considerable amount of time to complete, we
recommend that you begin the process well in advance of any update to Reef or
to later releases.

POOL_FULL
_________

One or more pools have reached their quota and are no longer allowing writes.

To see pool quotas and utilization, run the following command:

.. prompt:: bash $

   ceph df detail

If you opt to raise the pool quota, run the following commands:

.. prompt:: bash $

   ceph osd pool set-quota <poolname> max_objects <num-objects>
   ceph osd pool set-quota <poolname> max_bytes <num-bytes>

If not, delete some existing data to reduce utilization.

BLUEFS_SPILLOVER
________________

One or more OSDs that use the BlueStore back end have been allocated `db`
partitions (that is, storage space for metadata, normally on a faster device),
but because that space has been filled, metadata has "spilled over" onto the
slow device. This is not necessarily an error condition or even unexpected
behavior, but may result in degraded performance. If the administrator had
expected that all metadata would fit on the faster device, this alert indicates
that not enough space was provided.

To disable this alert on all OSDs, run the following command:

.. prompt:: bash $

   ceph config set osd bluestore_warn_on_bluefs_spillover false

Alternatively, to disable the alert on a specific OSD, run the following
command:

.. prompt:: bash $

   ceph config set osd.123 bluestore_warn_on_bluefs_spillover false

To secure more metadata space, you can destroy and reprovision the OSD in
question. This process involves data migration and recovery.

It might also be possible to expand the LVM logical volume that backs the `db`
storage. If the underlying LV has been expanded, you must stop the OSD daemon
and inform BlueFS of the device-size change by running the following command:

.. prompt:: bash $

   ceph-bluestore-tool bluefs-bdev-expand --path /var/lib/ceph/osd/ceph-$ID

BLUEFS_AVAILABLE_SPACE
______________________

To see how much space is free for BlueFS, run the following command:

.. prompt:: bash $

   ceph daemon osd.123 bluestore bluefs available

This will output up to three values: ``BDEV_DB free``, ``BDEV_SLOW free``, and
``available_from_bluestore``. ``BDEV_DB`` and ``BDEV_SLOW`` report the amount
of space that has been acquired by BlueFS and is now considered free. The value
``available_from_bluestore`` indicates the ability of BlueStore to relinquish
more space to BlueFS.  It is normal for this value to differ from the amount of
BlueStore free space, because the BlueFS allocation unit is typically larger
than the BlueStore allocation unit.  This means that only part of the BlueStore
free space will be available for BlueFS.

BLUEFS_LOW_SPACE
_________________

If BlueFS is running low on available free space and there is not much free
space available from BlueStore (in other words, `available_from_bluestore` has
a low value), consider reducing the BlueFS allocation unit size. To simulate
available space when the allocation unit is different, run the following
command: 

.. prompt:: bash $

   ceph daemon osd.123 bluestore bluefs available <alloc-unit-size>

BLUESTORE_FRAGMENTATION
_______________________

As BlueStore operates, the free space on the underlying storage will become
fragmented.  This is normal and unavoidable, but excessive fragmentation causes
slowdown.  To inspect BlueStore fragmentation, run the following command:

.. prompt:: bash $

   ceph daemon osd.123 bluestore allocator score block

The fragmentation score is given in a [0-1] range.
[0.0 .. 0.4] tiny fragmentation
[0.4 .. 0.7] small, acceptable fragmentation
[0.7 .. 0.9] considerable, but safe fragmentation
[0.9 .. 1.0] severe fragmentation, might impact BlueFS's ability to get space from BlueStore

To see a detailed report of free fragments, run the following command:

.. prompt:: bash $

   ceph daemon osd.123 bluestore allocator dump block

For OSD processes that are not currently running, fragmentation can be
inspected with `ceph-bluestore-tool`. To see the fragmentation score, run the
following command:

.. prompt:: bash $

   ceph-bluestore-tool --path /var/lib/ceph/osd/ceph-123 --allocator block free-score

To dump detailed free chunks, run the following command:

.. prompt:: bash $

   ceph-bluestore-tool --path /var/lib/ceph/osd/ceph-123 --allocator block free-dump

BLUESTORE_LEGACY_STATFS
_______________________

One or more OSDs have BlueStore volumes that were created prior to the
Nautilus release. (In Nautilus, BlueStore tracks its internal usage
statistics on a granular, per-pool basis.)

If *all* OSDs
are older than Nautilus, this means that the per-pool metrics are
simply unavailable. But if there is a mixture of pre-Nautilus and
post-Nautilus OSDs, the cluster usage statistics reported by ``ceph
df`` will be inaccurate.

The old OSDs can be updated to use the new usage-tracking scheme by stopping
each OSD, running a repair operation, and then restarting the OSD. For example,
to update ``osd.123``, run the following commands:

.. prompt:: bash $

   systemctl stop ceph-osd@123
   ceph-bluestore-tool repair --path /var/lib/ceph/osd/ceph-123
   systemctl start ceph-osd@123

To disable this alert, run the following command:

.. prompt:: bash $

   ceph config set global bluestore_warn_on_legacy_statfs false

BLUESTORE_NO_PER_POOL_OMAP
__________________________

One or more OSDs have volumes that were created prior to the Octopus release.
(In Octopus and later releases, BlueStore tracks omap space utilization by
pool.)

If there are any BlueStore OSDs that do not have the new tracking enabled, the
cluster will report an approximate value for per-pool omap usage based on the
most recent deep scrub.

The OSDs can be updated to track by pool by stopping each OSD, running a repair
operation, and then restarting the OSD. For example, to update ``osd.123``, run
the following commands:

.. prompt:: bash $

   systemctl stop ceph-osd@123
   ceph-bluestore-tool repair --path /var/lib/ceph/osd/ceph-123
   systemctl start ceph-osd@123

To disable this alert, run the following command:

.. prompt:: bash $

   ceph config set global bluestore_warn_on_no_per_pool_omap false

BLUESTORE_NO_PER_PG_OMAP
__________________________

One or more OSDs have volumes that were created prior to Pacific.  (In Pacific
and later releases Bluestore tracks omap space utilitzation by Placement Group
(PG).)

Per-PG omap allows faster PG removal when PGs migrate.

The older OSDs can be updated to track by PG by stopping each OSD, running a
repair operation, and then restarting the OSD. For example, to update
``osd.123``, run the following commands:

.. prompt:: bash $

   systemctl stop ceph-osd@123
   ceph-bluestore-tool repair --path /var/lib/ceph/osd/ceph-123
   systemctl start ceph-osd@123

To disable this alert, run the following command:

.. prompt:: bash $

   ceph config set global bluestore_warn_on_no_per_pg_omap false


BLUESTORE_DISK_SIZE_MISMATCH
____________________________

One or more BlueStore OSDs have an internal inconsistency between the size of
the physical device and the metadata that tracks its size. This inconsistency
can lead to the OSD(s) crashing in the future.

The OSDs that have this inconsistency should be destroyed and reprovisioned. Be
very careful to execute this procedure on only one OSD at a time, so as to
minimize the risk of losing any data. To execute this procedure, where ``$N``
is the OSD that has the inconsistency, run the following commands:

.. prompt:: bash $

   ceph osd out osd.$N
   while ! ceph osd safe-to-destroy osd.$N ; do sleep 1m ; done
   ceph osd destroy osd.$N
   ceph-volume lvm zap /path/to/device
   ceph-volume lvm create --osd-id $N --data /path/to/device

.. note::

   Wait for this recovery procedure to completely on one OSD before running it
   on the next.

BLUESTORE_NO_COMPRESSION
________________________

One or more OSDs is unable to load a BlueStore compression plugin.  This issue
might be caused by a broken installation, in which the ``ceph-osd`` binary does
not match the compression plugins. Or it might be caused by a recent upgrade in
which the ``ceph-osd`` daemon was not restarted.

To resolve this issue, verify that all of the packages on the host that is
running the affected OSD(s) are correctly installed and that the OSD daemon(s)
have been restarted. If the problem persists, check the OSD log for information
about the source of the problem.

BLUESTORE_SPURIOUS_READ_ERRORS
______________________________

One or more BlueStore OSDs detect spurious read errors on the main device.
BlueStore has recovered from these errors by retrying disk reads.  This alert
might indicate issues with underlying hardware, issues with the I/O subsystem,
or something similar.  In theory, such issues can cause permanent data
corruption.  Some observations on the root cause of spurious read errors can be
found here: https://tracker.ceph.com/issues/22464

This alert does not require an immediate response, but the affected host might
need additional attention: for example, upgrading the host to the latest
OS/kernel versions and implementing hardware-resource-utilization monitoring.

To disable this alert on all OSDs, run the following command:

.. prompt:: bash $

   ceph config set osd bluestore_warn_on_spurious_read_errors false

Or, to disable this alert on a specific OSD, run the following command:

.. prompt:: bash $

   ceph config set osd.123 bluestore_warn_on_spurious_read_errors false

Device health
-------------

DEVICE_HEALTH
_____________

One or more devices is expected to fail soon, where the warning
threshold is controlled by the ``mgr/devicehealth/warn_threshold``
config option.

This warning only applies to OSDs that are currently marked "in", so
the expected response to this failure is to mark the device "out" so
that data is migrated off of the device, and then to remove the
hardware from the system.  Note that the marking out is normally done
automatically if ``mgr/devicehealth/self_heal`` is enabled based on
the ``mgr/devicehealth/mark_out_threshold``.

Device health can be checked with:

.. prompt:: bash $

   ceph device info <device-id>

Device life expectancy is set by a prediction model run by
the mgr or an by external tool via the command:

.. prompt:: bash $

   ceph device set-life-expectancy <device-id> <from> <to>

You can change the stored life expectancy manually, but that usually
doesn't accomplish anything as whatever tool originally set it will
probably set it again, and changing the stored value does not affect
the actual health of the hardware device.

DEVICE_HEALTH_IN_USE
____________________

One or more devices is expected to fail soon and has been marked "out"
of the cluster based on ``mgr/devicehealth/mark_out_threshold``, but it
is still participating in one more PGs.  This may be because it was
only recently marked "out" and data is still migrating, or because data
cannot be migrated off for some reason (e.g., the cluster is nearly
full, or the CRUSH hierarchy is such that there isn't another suitable
OSD to migrate the data too).

This message can be silenced by disabling the self heal behavior
(setting ``mgr/devicehealth/self_heal`` to false), by adjusting the
``mgr/devicehealth/mark_out_threshold``, or by addressing what is
preventing data from being migrated off of the ailing device.

DEVICE_HEALTH_TOOMANY
_____________________

Too many devices is expected to fail soon and the
``mgr/devicehealth/self_heal`` behavior is enabled, such that marking
out all of the ailing devices would exceed the clusters
``mon_osd_min_in_ratio`` ratio that prevents too many OSDs from being
automatically marked "out".

This generally indicates that too many devices in your cluster are
expected to fail soon and you should take action to add newer
(healthier) devices before too many devices fail and data is lost.

The health message can also be silenced by adjusting parameters like
``mon_osd_min_in_ratio`` or ``mgr/devicehealth/mark_out_threshold``,
but be warned that this will increase the likelihood of unrecoverable
data loss in the cluster.


Data health (pools & placement groups)
--------------------------------------

PG_AVAILABILITY
_______________

Data availability is reduced, meaning that the cluster is unable to
service potential read or write requests for some data in the cluster.
Specifically, one or more PGs is in a state that does not allow IO
requests to be serviced.  Problematic PG states include *peering*,
*stale*, *incomplete*, and the lack of *active* (if those conditions do not clear
quickly).

Detailed information about which PGs are affected is available from:

.. prompt:: bash $

   ceph health detail

In most cases the root cause is that one or more OSDs is currently
down; see the discussion for ``OSD_DOWN`` above.

The state of specific problematic PGs can be queried with:

.. prompt:: bash $

   ceph tell <pgid> query

PG_DEGRADED
___________

Data redundancy is reduced for some data, meaning the cluster does not
have the desired number of replicas for all data (for replicated
pools) or erasure code fragments (for erasure coded pools).
Specifically, one or more PGs:

* has the *degraded* or *undersized* flag set, meaning there are not
  enough instances of that placement group in the cluster;
* has not had the *clean* flag set for some time.

Detailed information about which PGs are affected is available from:

.. prompt:: bash $

   ceph health detail

In most cases the root cause is that one or more OSDs is currently
down; see the discussion for ``OSD_DOWN`` above.

The state of specific problematic PGs can be queried with:

.. prompt:: bash $

   ceph tell <pgid> query


PG_RECOVERY_FULL
________________

Data redundancy may be reduced or at risk for some data due to a lack
of free space in the cluster.  Specifically, one or more PGs has the
*recovery_toofull* flag set, meaning that the
cluster is unable to migrate or recover data because one or more OSDs
is above the *full* threshold.

See the discussion for *OSD_FULL* above for steps to resolve this condition.

PG_BACKFILL_FULL
________________

Data redundancy may be reduced or at risk for some data due to a lack
of free space in the cluster.  Specifically, one or more PGs has the
*backfill_toofull* flag set, meaning that the
cluster is unable to migrate or recover data because one or more OSDs
is above the *backfillfull* threshold.

See the discussion for *OSD_BACKFILLFULL* above for
steps to resolve this condition.

.. _rados-operations-health-checks-pg-damaged:

PG_DAMAGED
__________

Data scrubbing has discovered some problems with data consistency in
the cluster.  Specifically, one or more PGs has the *inconsistent* or
*snaptrim_error* flag is set, indicating an earlier scrub operation
found a problem, or that the *repair* flag is set, meaning a repair
for such an inconsistency is currently in progress.

See :doc:`pg-repair` for more information.

OSD_SCRUB_ERRORS
________________

Recent OSD scrubs have uncovered inconsistencies. This error is generally
paired with *PG_DAMAGED* (see :ref:`PG_DAMAGE <rados-operations-health-checks-pg-damaged>`).

See :doc:`pg-repair` for more information.

OSD_TOO_MANY_REPAIRS
____________________

When a read error occurs and another replica is available it is used to repair
the error immediately, so that the client can get the object data.  Scrub
handles errors for data at rest.  In order to identify possible failing disks
that aren't seeing scrub errors, a count of read repairs is maintained.  If
it exceeds a config value threshold *mon_osd_warn_num_repaired* default 10,
this health warning is generated.

LARGE_OMAP_OBJECTS
__________________

One or more pools contain large omap objects as determined by
``osd_deep_scrub_large_omap_object_key_threshold`` (threshold for number of keys
to determine a large omap object) or
``osd_deep_scrub_large_omap_object_value_sum_threshold`` (the threshold for
summed size (bytes) of all key values to determine a large omap object) or both.
More information on the object name, key count, and size in bytes can be found
by searching the cluster log for 'Large omap object found'. Large omap objects
can be caused by RGW bucket index objects that do not have automatic resharding
enabled. Please see :ref:`RGW Dynamic Bucket Index Resharding
<rgw_dynamic_bucket_index_resharding>` for more information on resharding.

The thresholds can be adjusted with:

.. prompt:: bash $

   ceph config set osd osd_deep_scrub_large_omap_object_key_threshold <keys>
   ceph config set osd osd_deep_scrub_large_omap_object_value_sum_threshold <bytes>

CACHE_POOL_NEAR_FULL
____________________

A cache tier pool is nearly full.  Full in this context is determined
by the ``target_max_bytes`` and ``target_max_objects`` properties on
the cache pool.  Once the pool reaches the target threshold, write
requests to the pool may block while data is flushed and evicted
from the cache, a state that normally leads to very high latencies and
poor performance.

The cache pool target size can be adjusted with:

.. prompt:: bash $

   ceph osd pool set <cache-pool-name> target_max_bytes <bytes>
   ceph osd pool set <cache-pool-name> target_max_objects <objects>

Normal cache flush and evict activity may also be throttled due to reduced
availability or performance of the base tier, or overall cluster load.

TOO_FEW_PGS
___________

The number of PGs in use in the cluster is below the configurable
threshold of ``mon_pg_warn_min_per_osd`` PGs per OSD.  This can lead
to suboptimal distribution and balance of data across the OSDs in
the cluster, and similarly reduce overall performance.

This may be an expected condition if data pools have not yet been
created.

The PG count for existing pools can be increased or new pools can be created.
Please refer to :ref:`choosing-number-of-placement-groups` for more
information.

POOL_PG_NUM_NOT_POWER_OF_TWO
____________________________

One or more pools has a ``pg_num`` value that is not a power of two.
Although this is not strictly incorrect, it does lead to a less
balanced distribution of data because some PGs have roughly twice as
much data as others.

This is easily corrected by setting the ``pg_num`` value for the
affected pool(s) to a nearby power of two:

.. prompt:: bash $

   ceph osd pool set <pool-name> pg_num <value>

This health warning can be disabled with:

.. prompt:: bash $

   ceph config set global mon_warn_on_pool_pg_num_not_power_of_two false

POOL_TOO_FEW_PGS
________________

One or more pools should probably have more PGs, based on the amount
of data that is currently stored in the pool.  This can lead to
suboptimal distribution and balance of data across the OSDs in the
cluster, and similarly reduce overall performance.  This warning is
generated if the ``pg_autoscale_mode`` property on the pool is set to
``warn``.

To disable the warning, you can disable auto-scaling of PGs for the
pool entirely with:

.. prompt:: bash $

   ceph osd pool set <pool-name> pg_autoscale_mode off

To allow the cluster to automatically adjust the number of PGs,:

.. prompt:: bash $

   ceph osd pool set <pool-name> pg_autoscale_mode on

You can also manually set the number of PGs for the pool to the
recommended amount with:

.. prompt:: bash $

   ceph osd pool set <pool-name> pg_num <new-pg-num>

Please refer to :ref:`choosing-number-of-placement-groups` and
:ref:`pg-autoscaler` for more information.

TOO_MANY_PGS
____________

The number of PGs in use in the cluster is above the configurable
threshold of ``mon_max_pg_per_osd`` PGs per OSD.  If this threshold is
exceed the cluster will not allow new pools to be created, pool `pg_num` to
be increased, or pool replication to be increased (any of which would lead to
more PGs in the cluster).  A large number of PGs can lead
to higher memory utilization for OSD daemons, slower peering after
cluster state changes (like OSD restarts, additions, or removals), and
higher load on the Manager and Monitor daemons.

The simplest way to mitigate the problem is to increase the number of
OSDs in the cluster by adding more hardware.  Note that the OSD count
used for the purposes of this health check is the number of "in" OSDs,
so marking "out" OSDs "in" (if there are any) can also help:

.. prompt:: bash $

   ceph osd in <osd id(s)>

Please refer to :ref:`choosing-number-of-placement-groups` for more
information.

POOL_TOO_MANY_PGS
_________________

One or more pools should probably have more PGs, based on the amount
of data that is currently stored in the pool.  This can lead to higher
memory utilization for OSD daemons, slower peering after cluster state
changes (like OSD restarts, additions, or removals), and higher load
on the Manager and Monitor daemons.  This warning is generated if the
``pg_autoscale_mode`` property on the pool is set to ``warn``.

To disable the warning, you can disable auto-scaling of PGs for the
pool entirely with:

.. prompt:: bash $

   ceph osd pool set <pool-name> pg_autoscale_mode off

To allow the cluster to automatically adjust the number of PGs,:

.. prompt:: bash $

   ceph osd pool set <pool-name> pg_autoscale_mode on

You can also manually set the number of PGs for the pool to the
recommended amount with:

.. prompt:: bash $

   ceph osd pool set <pool-name> pg_num <new-pg-num>

Please refer to :ref:`choosing-number-of-placement-groups` and
:ref:`pg-autoscaler` for more information.

POOL_TARGET_SIZE_BYTES_OVERCOMMITTED
____________________________________

One or more pools have a ``target_size_bytes`` property set to
estimate the expected size of the pool,
but the value(s) exceed the total available storage (either by
themselves or in combination with other pools' actual usage).

This is usually an indication that the ``target_size_bytes`` value for
the pool is too large and should be reduced or set to zero with:

.. prompt:: bash $

   ceph osd pool set <pool-name> target_size_bytes 0

For more information, see :ref:`specifying_pool_target_size`.

POOL_HAS_TARGET_SIZE_BYTES_AND_RATIO
____________________________________

One or more pools have both ``target_size_bytes`` and
``target_size_ratio`` set to estimate the expected size of the pool.
Only one of these properties should be non-zero. If both are set,
``target_size_ratio`` takes precedence and ``target_size_bytes`` is
ignored.

To reset ``target_size_bytes`` to zero:

.. prompt:: bash $

   ceph osd pool set <pool-name> target_size_bytes 0

For more information, see :ref:`specifying_pool_target_size`.

TOO_FEW_OSDS
____________

The number of OSDs in the cluster is below the configurable
threshold of ``osd_pool_default_size``.

SMALLER_PGP_NUM
_______________

One or more pools has a ``pgp_num`` value less than ``pg_num``.  This
is normally an indication that the PG count was increased without
also increasing the placement behavior.

This is sometimes done deliberately to separate out the `split` step
when the PG count is adjusted from the data migration that is needed
when ``pgp_num`` is changed.

This is normally resolved by setting ``pgp_num`` to match ``pg_num``,
triggering the data migration, with:

.. prompt:: bash $

   ceph osd pool set <pool> pgp_num <pg-num-value>

MANY_OBJECTS_PER_PG
___________________

One or more pools has an average number of objects per PG that is
significantly higher than the overall cluster average.  The specific
threshold is controlled by the ``mon_pg_warn_max_object_skew``
configuration value.

This is usually an indication that the pool(s) containing most of the
data in the cluster have too few PGs, and/or that other pools that do
not contain as much data have too many PGs.  See the discussion of
*TOO_MANY_PGS* above.

The threshold can be raised to silence the health warning by adjusting
the ``mon_pg_warn_max_object_skew`` config option on the managers.

The health warning will be silenced for a particular pool if
``pg_autoscale_mode`` is set to ``on``.

POOL_APP_NOT_ENABLED
____________________

A pool exists that contains one or more objects but has not been
tagged for use by a particular application.

Resolve this warning by labeling the pool for use by an application.  For
example, if the pool is used by RBD,:

.. prompt:: bash $

   rbd pool init <poolname>

If the pool is being used by a custom application 'foo', you can also label
via the low-level command:

.. prompt:: bash $

   ceph osd pool application enable foo

For more information, see :ref:`associate-pool-to-application`.

POOL_FULL
_________

One or more pools has reached (or is very close to reaching) its
quota.  The threshold to trigger this error condition is controlled by
the ``mon_pool_quota_crit_threshold`` configuration option.

Pool quotas can be adjusted up or down (or removed) with:

.. prompt:: bash $

   ceph osd pool set-quota <pool> max_bytes <bytes>
   ceph osd pool set-quota <pool> max_objects <objects>

Setting the quota value to 0 will disable the quota.

POOL_NEAR_FULL
______________

One or more pools is approaching a configured fullness threshold.

One threshold that can trigger this warning condition is the
``mon_pool_quota_warn_threshold`` configuration option.

Pool quotas can be adjusted up or down (or removed) with:

.. prompt:: bash $

   ceph osd pool set-quota <pool> max_bytes <bytes>
   ceph osd pool set-quota <pool> max_objects <objects>

Setting the quota value to 0 will disable the quota.

Other thresholds that can trigger the above two warning conditions are
``mon_osd_nearfull_ratio`` and ``mon_osd_full_ratio``.  Visit the
:ref:`storage-capacity` and :ref:`no-free-drive-space` documents for details
and resolution.

OBJECT_MISPLACED
________________

One or more objects in the cluster is not stored on the node the
cluster would like it to be stored on.  This is an indication that
data migration due to some recent cluster change has not yet completed.

Misplaced data is not a dangerous condition in and of itself; data
consistency is never at risk, and old copies of objects are never
removed until the desired number of new copies (in the desired
locations) are present.

OBJECT_UNFOUND
______________

One or more objects in the cluster cannot be found.  Specifically, the
OSDs know that a new or updated copy of an object should exist, but a
copy of that version of the object has not been found on OSDs that are
currently online.

Read or write requests to unfound objects will block.

Ideally, a down OSD can be brought back online that has the more
recent copy of the unfound object.  Candidate OSDs can be identified from the
peering state for the PG(s) responsible for the unfound object:

.. prompt:: bash $

   ceph tell <pgid> query

If the latest copy of the object is not available, the cluster can be
told to roll back to a previous version of the object. See
:ref:`failures-osd-unfound` for more information.

SLOW_OPS
________

One or more OSD or monitor requests is taking a long time to process.  This can
be an indication of extreme load, a slow storage device, or a software
bug.

The request queue for the daemon in question can be queried with the
following command, executed from the daemon's host:

.. prompt:: bash $

   ceph daemon osd.<id> ops

A summary of the slowest recent requests can be seen with:

.. prompt:: bash $

   ceph daemon osd.<id> dump_historic_ops

The location of an OSD can be found with:

.. prompt:: bash $

   ceph osd find osd.<id>

PG_NOT_SCRUBBED
_______________

One or more PGs has not been scrubbed recently.  PGs are normally scrubbed
within every configured interval specified by
:confval:`osd_scrub_max_interval` globally. This
interval can be overridden on per-pool basis with
:confval:`scrub_max_interval`. The warning triggers when
``mon_warn_pg_not_scrubbed_ratio`` percentage of interval has elapsed without a
scrub since it was due.

PGs will not scrub if they are not flagged as *clean*, which may
happen if they are misplaced or degraded (see *PG_AVAILABILITY* and
*PG_DEGRADED* above).

You can manually initiate a scrub of a clean PG with::

  ceph pg scrub <pgid>

PG_NOT_DEEP_SCRUBBED
____________________

One or more PGs has not been deep scrubbed recently.  PGs are normally
scrubbed every :confval:`osd_deep_scrub_interval` seconds, and this warning
triggers when ``mon_warn_pg_not_deep_scrubbed_ratio`` percentage of interval has elapsed
without a scrub since it was due.

PGs will not (deep) scrub if they are not flagged as *clean*, which may
happen if they are misplaced or degraded (see *PG_AVAILABILITY* and
*PG_DEGRADED* above).

You can manually initiate a scrub of a clean PG with:

.. prompt:: bash $

   ceph pg deep-scrub <pgid>


PG_SLOW_SNAP_TRIMMING
_____________________

The snapshot trim queue for one or more PGs has exceeded the
configured warning threshold.  This indicates that either an extremely
large number of snapshots were recently deleted, or that the OSDs are
unable to trim snapshots quickly enough to keep up with the rate of
new snapshot deletions.

The warning threshold is controlled by the
``mon_osd_snap_trim_queue_warn_on`` option (default: 32768).

This warning may trigger if OSDs are under excessive load and unable
to keep up with their background work, or if the OSDs' internal
metadata database is heavily fragmented and unable to perform.  It may
also indicate some other performance issue with the OSDs.

The exact size of the snapshot trim queue is reported by the
``snaptrimq_len`` field of ``ceph pg ls -f json-detail``.

Miscellaneous
-------------

RECENT_CRASH
____________

One or more Ceph daemons has crashed recently, and the crash has not
yet been archived (acknowledged) by the administrator.  This may
indicate a software bug, a hardware problem (e.g., a failing disk), or
some other problem.

New crashes can be listed with:

.. prompt:: bash $

   ceph crash ls-new

Information about a specific crash can be examined with:

.. prompt:: bash $

   ceph crash info <crash-id>

This warning can be silenced by "archiving" the crash (perhaps after
being examined by an administrator) so that it does not generate this
warning:

.. prompt:: bash $

   ceph crash archive <crash-id>

Similarly, all new crashes can be archived with:

.. prompt:: bash $

   ceph crash archive-all

Archived crashes will still be visible via ``ceph crash ls`` but not
``ceph crash ls-new``.

The time period for what "recent" means is controlled by the option
``mgr/crash/warn_recent_interval`` (default: two weeks).

These warnings can be disabled entirely with:

.. prompt:: bash $

   ceph config set mgr/crash/warn_recent_interval 0

RECENT_MGR_MODULE_CRASH
_______________________

One or more ceph-mgr modules has crashed recently, and the crash as
not yet been archived (acknowledged) by the administrator.  This
generally indicates a software bug in one of the software modules run
inside the ceph-mgr daemon.  Although the module that experienced the
problem maybe be disabled as a result, the function of other modules
is normally unaffected.

As with the *RECENT_CRASH* health alert, the crash can be inspected with:

.. prompt:: bash $

   ceph crash info <crash-id>

This warning can be silenced by "archiving" the crash (perhaps after
being examined by an administrator) so that it does not generate this
warning:

.. prompt:: bash $

   ceph crash archive <crash-id>

Similarly, all new crashes can be archived with:

.. prompt:: bash $

   ceph crash archive-all

Archived crashes will still be visible via ``ceph crash ls`` but not
``ceph crash ls-new``.

The time period for what "recent" means is controlled by the option
``mgr/crash/warn_recent_interval`` (default: two weeks).

These warnings can be disabled entirely with:

.. prompt:: bash $

   ceph config set mgr/crash/warn_recent_interval 0

TELEMETRY_CHANGED
_________________

Telemetry has been enabled, but the contents of the telemetry report
have changed since that time, so telemetry reports will not be sent.

The Ceph developers periodically revise the telemetry feature to
include new and useful information, or to remove information found to
be useless or sensitive.  If any new information is included in the
report, Ceph will require the administrator to re-enable telemetry to
ensure they have an opportunity to (re)review what information will be
shared.

To review the contents of the telemetry report:

.. prompt:: bash $

   ceph telemetry show

Note that the telemetry report consists of several optional channels
that may be independently enabled or disabled.  For more information, see
:ref:`telemetry`.

To re-enable telemetry (and make this warning go away):

.. prompt:: bash $

   ceph telemetry on

To disable telemetry (and make this warning go away):

.. prompt:: bash $

   ceph telemetry off

AUTH_BAD_CAPS
_____________

One or more auth users has capabilities that cannot be parsed by the
monitor.  This generally indicates that the user will not be
authorized to perform any action with one or more daemon types.

This error is mostly likely to occur after an upgrade if the
capabilities were set with an older version of Ceph that did not
properly validate their syntax, or if the syntax of the capabilities
has changed.

The user in question can be removed with:

.. prompt:: bash $

   ceph auth rm <entity-name>

(This will resolve the health alert, but obviously clients will not be
able to authenticate as that user.)

Alternatively, the capabilities for the user can be updated with:

.. prompt:: bash $

   ceph auth <entity-name> <daemon-type> <caps> [<daemon-type> <caps> ...]

For more information about auth capabilities, see :ref:`user-management`.

OSD_NO_DOWN_OUT_INTERVAL
________________________

The ``mon_osd_down_out_interval`` option is set to zero, which means
that the system will not automatically perform any repair or healing
operations after an OSD fails.  Instead, an administrator (or some
other external entity) will need to manually mark down OSDs as 'out'
(i.e., via ``ceph osd out <osd-id>``) in order to trigger recovery.

This option is normally set to five or ten minutes--enough time for a
host to power-cycle or reboot.

This warning can silenced by setting the
``mon_warn_on_osd_down_out_interval_zero`` to false:

.. prompt:: bash $

   ceph config global mon mon_warn_on_osd_down_out_interval_zero false

DASHBOARD_DEBUG
_______________

The Dashboard debug mode is enabled. This means, if there is an error
while processing a REST API request, the HTTP error response contains
a Python traceback. This behaviour should be disabled in production
environments because such a traceback might contain and expose sensible
information.

The debug mode can be disabled with:

.. prompt:: bash $

   ceph dashboard debug disable
