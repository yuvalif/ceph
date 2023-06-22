"""
Copyright (C) 2019 SUSE

LGPL2.1.  See file COPYING.
"""
import errno
import json
import sqlite3
from typing import Any, Dict, Optional, Tuple
from .fs.schedule_client import SnapSchedClient
from mgr_module import MgrModule, CLIReadCommand, CLIWriteCommand, Option
from mgr_util import CephfsConnectionException
from threading import Event


class Module(MgrModule):
    MODULE_OPTIONS = [
        Option(
            'allow_m_granularity',
            type='bool',
            default=False,
            desc='allow minute scheduled snapshots',
            runtime=True,
        ),
        Option(
            'dump_on_update',
            type='bool',
            default=False,
            desc='dump database to debug log on update',
            runtime=True,
        ),

    ]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super(Module, self).__init__(*args, **kwargs)
        self._initialized = Event()
        self.client = SnapSchedClient(self)

    def subvolume_exist(self, fs: str, subvol: str) -> bool:
        rc, subvolumes, err = self.remote('volumes', 'subvolume_ls', fs, None)
        if rc == 0:
            for svj in json.loads(subvolumes):
                if subvol == svj['name']:
                    return True

        return False

    def subvolume_flavor(self, fs: str, subvol: str) -> str:
        rc, subvol_info, err = self.remote('volumes', 'subvolume_info', fs, subvol, None)
        svi_json = json.loads(subvol_info)
        return svi_json.get('flavor', 'bad_flavor')  # "1" or "2" etc.

    def resolve_subvolume_path(self, fs: str, subvol: Optional[str], path: str) -> Tuple[int, str]:
        if not subvol:
            return 0, path, ''

        rc = -1
        subvol_path = ''
        if self.subvolume_exist(fs, subvol):
            rc, subvol_path, err = self.remote('volumes', 'subvolume_getpath', fs, subvol, None)
            if rc != 0:
                return rc, '', f'Could not resolve subvol:{subvol} path in fs:{fs}'
            else:
                subvol_flavor = self.subvolume_flavor(fs, subvol)
                if subvol_flavor == "1":  # v1
                    return 0, subvol_path, f'Ignoring user specified path:{path} for subvol'
                if subvol_flavor == "2":  # v2
                    err = '';
                    if path != "/..":
                        err = f'Ignoring user specified path:{path} for subvol'
                    return 0, subvol_path + "/..", err

                return -errno.EINVAL, '', f'Unhandled subvol flavor:{subvol_flavor}'
        else:
            return -errno.EINVAL, '', f'No such subvol:{subvol}'

    @property
    def default_fs(self) -> str:
        fs_map = self.get('fs_map')
        if fs_map['filesystems']:
            return fs_map['filesystems'][0]['mdsmap']['fs_name']
        else:
            self.log.error('No filesystem instance could be found.')
            raise CephfsConnectionException(
                -errno.ENOENT, "no filesystem found")

    def has_fs(self, fs_name: str) -> bool:
        return fs_name in self.client.get_all_filesystems()

    def serve(self) -> None:
        self._initialized.set()

    def handle_command(self, inbuf: str, cmd: Dict[str, str]) -> Tuple[int, str, str]:
        self._initialized.wait()
        return -errno.EINVAL, "", "Unknown command"

    @CLIReadCommand('fs snap-schedule status')
    def snap_schedule_get(self,
                          path: str = '/',
                          subvol: Optional[str] = None,
                          fs: Optional[str] = None,
                          format: Optional[str] = 'plain') -> Tuple[int, str, str]:
        '''
        List current snapshot schedules
        '''
        use_fs = fs if fs else self.default_fs
        if not self.has_fs(use_fs):
            return -errno.EINVAL, '', f"no such filesystem: {use_fs}"
        errstr = 'Success'
        try:
            rc, abs_path, errstr = self.resolve_subvolume_path(use_fs, subvol, path)
            if rc != 0:
                return rc, '', errstr
            ret_scheds = self.client.get_snap_schedules(use_fs, abs_path)
        except CephfsConnectionException as e:
            return e.to_tuple()
        if format == 'json':
            json_report = ','.join([ret_sched.report_json() for ret_sched in ret_scheds])
            return 0, f'[{json_report}]', ''
        self.log.info(errstr)
        return 0, '\n===\n'.join([ret_sched.report() for ret_sched in ret_scheds]), ''

    @CLIReadCommand('fs snap-schedule list')
    def snap_schedule_list(self, path: str,
                           subvol: Optional[str] = None,
                           recursive: bool = False,
                           fs: Optional[str] = None,
                           format: Optional[str] = 'plain') -> Tuple[int, str, str]:
        '''
        Get current snapshot schedule for <path>
        '''
        errstr = 'Success'
        try:
            use_fs = fs if fs else self.default_fs
            if not self.has_fs(use_fs):
                return -errno.EINVAL, '', f"no such filesystem: {use_fs}"
            rc, abs_path, errstr = self.resolve_subvolume_path(use_fs, subvol, path)
            if rc != 0:
                return rc, errstr, ''
            scheds = self.client.list_snap_schedules(use_fs, abs_path, recursive)
            self.log.debug(f'recursive is {recursive}')
        except CephfsConnectionException as e:
            return e.to_tuple()
        if not scheds:
            if format == 'json':
                output: Dict[str, str] = {}
                return 0, json.dumps(output), ''
            return -errno.ENOENT, '', f'SnapSchedule for {abs_path} not found'
        if format == 'json':
            # json_list = ','.join([sched.json_list() for sched in scheds])
            schedule_list = [sched.schedule for sched in scheds]
            retention_list = [sched.retention for sched in scheds]
            out = {'path': abs_path, 'schedule': schedule_list, 'retention': retention_list}
            return 0, json.dumps(out), ''
        self.log.info(errstr)
        return 0, '\n'.join([str(sched) for sched in scheds]), ''

    @CLIWriteCommand('fs snap-schedule add')
    def snap_schedule_add(self,
                          path: str,
                          snap_schedule: str,
                          start: Optional[str] = None,
                          fs: Optional[str] = None,
                          subvol: Optional[str] = None) -> Tuple[int, str, str]:
        '''
        Set a snapshot schedule for <path>
        '''
        errstr = 'Success'
        try:
            use_fs = fs if fs else self.default_fs
            if not self.has_fs(use_fs):
                return -errno.EINVAL, '', f"no such filesystem: {use_fs}"
            rc, abs_path, errstr = self.resolve_subvolume_path(use_fs, subvol, path)
            if rc != 0:
                return rc, '', errstr
            self.client.store_snap_schedule(use_fs,
                                            abs_path,
                                            (abs_path, snap_schedule,
                                             use_fs, abs_path, start, subvol))
            suc_msg = f'Schedule set for path {abs_path}'
        except sqlite3.IntegrityError:
            existing_scheds = self.client.get_snap_schedules(use_fs, abs_path)
            report = [s.report() for s in existing_scheds]
            error_msg = f'Found existing schedule {report}'
            self.log.error(error_msg)
            return -errno.EEXIST, '', error_msg
        except ValueError as e:
            return -errno.ENOENT, '', str(e)
        except CephfsConnectionException as e:
            return e.to_tuple()
        self.log.info(errstr)
        return 0, suc_msg, ''

    @CLIWriteCommand('fs snap-schedule remove')
    def snap_schedule_rm(self,
                         path: str,
                         repeat: Optional[str] = None,
                         start: Optional[str] = None,
                         subvol: Optional[str] = None,
                         fs: Optional[str] = None) -> Tuple[int, str, str]:
        '''
        Remove a snapshot schedule for <path>
        '''
        errstr = 'Success'
        try:
            use_fs = fs if fs else self.default_fs
            if not self.has_fs(use_fs):
                return -errno.EINVAL, '', f"no such filesystem: {use_fs}"
            rc, abs_path, errstr = self.resolve_subvolume_path(use_fs, subvol, path)
            if rc != 0:
                return rc, '', errstr
            self.client.rm_snap_schedule(use_fs, abs_path, repeat, start)
        except CephfsConnectionException as e:
            return e.to_tuple()
        except ValueError as e:
            return -errno.ENOENT, '', str(e)
        self.log.info(errstr)
        return 0, 'Schedule removed for path {}'.format(abs_path), ''

    @CLIWriteCommand('fs snap-schedule retention add')
    def snap_schedule_retention_add(self,
                                    path: str,
                                    retention_spec_or_period: str,
                                    retention_count: Optional[str] = None,
                                    fs: Optional[str] = None,
                                    subvol: Optional[str] = None) -> Tuple[int, str, str]:
        '''
        Set a retention specification for <path>
        '''
        errstr = 'Success'
        try:
            use_fs = fs if fs else self.default_fs
            if not self.has_fs(use_fs):
                return -errno.EINVAL, '', f"no such filesystem: {use_fs}"
            rc, abs_path, errstr = self.resolve_subvolume_path(use_fs, subvol, path)
            if rc != 0:
                return rc, '', errstr
            self.client.add_retention_spec(use_fs, abs_path,
                                          retention_spec_or_period,
                                          retention_count)
        except CephfsConnectionException as e:
            return e.to_tuple()
        except ValueError as e:
            return -errno.ENOENT, '', str(e)
        self.log.info(errstr)
        return 0, 'Retention added to path {}'.format(abs_path), ''

    @CLIWriteCommand('fs snap-schedule retention remove')
    def snap_schedule_retention_rm(self,
                                   path: str,
                                   retention_spec_or_period: str,
                                   retention_count: Optional[str] = None,
                                   fs: Optional[str] = None,
                                   subvol: Optional[str] = None) -> Tuple[int, str, str]:
        '''
        Remove a retention specification for <path>
        '''
        errstr = 'Success'
        try:
            use_fs = fs if fs else self.default_fs
            if not self.has_fs(use_fs):
                return -errno.EINVAL, '', f"no such filesystem: {use_fs}"
            rc, abs_path, errstr = self.resolve_subvolume_path(use_fs, subvol, path)
            if rc != 0:
                return rc, '', errstr
            self.client.rm_retention_spec(use_fs, abs_path,
                                          retention_spec_or_period,
                                          retention_count)
        except CephfsConnectionException as e:
            return e.to_tuple()
        except ValueError as e:
            return -errno.ENOENT, '', str(e)
        self.log.info(errstr)
        return 0, 'Retention removed from path {}'.format(abs_path), ''

    @CLIWriteCommand('fs snap-schedule activate')
    def snap_schedule_activate(self,
                               path: str,
                               repeat: Optional[str] = None,
                               start: Optional[str] = None,
                               subvol: Optional[str] = None,
                               fs: Optional[str] = None) -> Tuple[int, str, str]:
        '''
        Activate a snapshot schedule for <path>
        '''
        errstr = 'Success'
        try:
            use_fs = fs if fs else self.default_fs
            if not self.has_fs(use_fs):
                return -errno.EINVAL, '', f"no such filesystem: {use_fs}"
            rc, abs_path, errstr = self.resolve_subvolume_path(use_fs, subvol, path)
            if rc != 0:
                return rc, '', errstr
            self.client.activate_snap_schedule(use_fs, abs_path, repeat, start)
        except CephfsConnectionException as e:
            return e.to_tuple()
        except ValueError as e:
            return -errno.ENOENT, '', str(e)
        self.log.info(errstr)
        return 0, 'Schedule activated for path {}'.format(abs_path), ''

    @CLIWriteCommand('fs snap-schedule deactivate')
    def snap_schedule_deactivate(self,
                                 path: str,
                                 repeat: Optional[str] = None,
                                 start: Optional[str] = None,
                                 subvol: Optional[str] = None,
                                 fs: Optional[str] = None) -> Tuple[int, str, str]:
        '''
        Deactivate a snapshot schedule for <path>
        '''
        errstr = 'Success'
        try:
            use_fs = fs if fs else self.default_fs
            if not self.has_fs(use_fs):
                return -errno.EINVAL, '', f"no such filesystem: {use_fs}"
            rc, abs_path, errstr = self.resolve_subvolume_path(use_fs, subvol, path)
            if rc != 0:
                return rc, '', errstr
            self.client.deactivate_snap_schedule(use_fs, abs_path, repeat, start)
        except CephfsConnectionException as e:
            return e.to_tuple()
        except ValueError as e:
            return -errno.ENOENT, '', str(e)
        self.log.info(errstr)
        return 0, 'Schedule deactivated for path {}'.format(abs_path), ''
