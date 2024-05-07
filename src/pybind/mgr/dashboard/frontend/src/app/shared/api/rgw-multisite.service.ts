import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { RgwRealm, RgwZone, RgwZonegroup } from '~/app/ceph/rgw/models/rgw-multisite';
import { RgwDaemonService } from './rgw-daemon.service';

@Injectable({
  providedIn: 'root'
})
export class RgwMultisiteService {
  private uiUrl = 'ui-api/rgw/multisite';
  private url = 'api/rgw/multisite';

  constructor(private http: HttpClient, public rgwDaemonService: RgwDaemonService) {}

  migrate(realm: RgwRealm, zonegroup: RgwZonegroup, zone: RgwZone) {
    return this.rgwDaemonService.request((params: HttpParams) => {
      params = params.appendAll({
        realm_name: realm.name,
        zonegroup_name: zonegroup.name,
        zone_name: zone.name,
        zonegroup_endpoints: zonegroup.endpoints,
        zone_endpoints: zone.endpoints,
        access_key: zone.system_key.access_key,
        secret_key: zone.system_key.secret_key
      });
      return this.http.put(`${this.uiUrl}/migrate`, null, { params: params });
    });
  }

  getSyncStatus() {
    return this.http.get(`${this.url}/sync_status`);
  }

  status() {
    return this.http.get(`${this.uiUrl}/status`);
  }

  setUpMultisiteReplication(
    realmName: string,
    zonegroupName: string,
    zonegroupEndpoints: string,
    zoneName: string,
    zoneEndpoints: string,
    username: string,
    cluster?: string
  ) {
    let params = new HttpParams()
      .set('realm_name', realmName)
      .set('zonegroup_name', zonegroupName)
      .set('zonegroup_endpoints', zonegroupEndpoints)
      .set('zone_name', zoneName)
      .set('zone_endpoints', zoneEndpoints)
      .set('username', username);

    if (cluster) {
      params = params.set('cluster_fsid', cluster);
    }

    return this.http.post(`${this.uiUrl}/multisite-replications`, null, { params: params });
  }
}
