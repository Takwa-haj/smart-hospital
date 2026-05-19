import _inheritsLoose from "@babel/runtime/helpers/inheritsLoose";
import { RxDBLeaderElectionPlugin } from "../leader-election/index.js";
import { RxReplicationState, startReplicationOnLeaderShip } from "../replication/index.js";
import { addRxPlugin } from "../../index.js";
import { Subject } from 'rxjs';
import { initDriveStructure } from "./init.js";
import { handleUpstreamBatch } from "./upstream.js";
import { fetchChanges } from "./downstream.js";
import { runInTransaction } from "./transaction.js";
import { ensureProcessNextTickIsSet } from "../replication-webrtc/connection-handler-simple-peer.js";
import { SignalingState } from "./signaling.js";
export * from "./google-drive-types.js";
export * from "./google-drive-helper.js";
export * from "./transaction.js";
export * from "./init.js";
export * from "./document-handling.js";
export * from "./multipart.js";
export * from "./downstream.js";
export * from "./upstream.js";
export * from "./signaling.js";
export var DEFAULT_TRANSACTION_TIMEOUT = 60 * 1000;
export var RxGoogleDriveReplicationState = /*#__PURE__*/function (_RxReplicationState) {
  /**
   * Only exists on live replication
   */

  function RxGoogleDriveReplicationState(googleDrive, driveStructure, replicationIdentifierHash, collection, pull, push, signalingOptions, live = true, retryTime = 1000 * 5, autoStart = true) {
    var _this;
    _this = _RxReplicationState.call(this, replicationIdentifierHash, collection, '_deleted', pull, push, live, retryTime, autoStart) || this;
    _this.googleDrive = googleDrive;
    _this.driveStructure = driveStructure;
    _this.replicationIdentifierHash = replicationIdentifierHash;
    _this.collection = collection;
    _this.pull = pull;
    _this.push = push;
    _this.signalingOptions = signalingOptions;
    _this.live = live;
    _this.retryTime = retryTime;
    _this.autoStart = autoStart;
    return _this;
  }

  /**
   * Notify other peers that something
   * has or might have changed so that
   * they can pull from their checkpoints.
   */
  _inheritsLoose(RxGoogleDriveReplicationState, _RxReplicationState);
  var _proto = RxGoogleDriveReplicationState.prototype;
  _proto.notifyPeers = async function notifyPeers() {
    if (this.signalingState) {
      await this.signalingState.pingPeers('RESYNC');
    }
  };
  return RxGoogleDriveReplicationState;
}(RxReplicationState);
export async function replicateGoogleDrive(options) {
  var collection = options.collection;
  addRxPlugin(RxDBLeaderElectionPlugin);
  var googleDriveOptionsWithDefaults = Object.assign({
    apiEndpoint: 'https://www.googleapis.com',
    transactionTimeout: DEFAULT_TRANSACTION_TIMEOUT
  }, options.googleDrive);
  var driveStructure = await initDriveStructure(googleDriveOptionsWithDefaults);
  var replicationState;
  var pullStream$ = new Subject();
  var replicationPrimitivesPull;
  options.live = typeof options.live === 'undefined' ? true : options.live;
  options.waitForLeadership = typeof options.waitForLeadership === 'undefined' ? true : options.waitForLeadership;
  if (options.pull) {
    replicationPrimitivesPull = {
      async handler(lastPulledCheckpoint, batchSize) {
        return runInTransaction(googleDriveOptionsWithDefaults, driveStructure, collection.schema.primaryPath, async () => {
          var changes = await fetchChanges(googleDriveOptionsWithDefaults, driveStructure, lastPulledCheckpoint, batchSize);
          return changes;
        });
      },
      batchSize: options.pull.batchSize,
      modifier: options.pull.modifier,
      stream$: pullStream$.asObservable(),
      initialCheckpoint: options.pull.initialCheckpoint
    };
  }
  var replicationPrimitivesPush;
  if (options.push) {
    replicationPrimitivesPush = {
      async handler(rows) {
        return runInTransaction(googleDriveOptionsWithDefaults, driveStructure, collection.schema.primaryPath, async () => {
          var conflicts = await handleUpstreamBatch(googleDriveOptionsWithDefaults, driveStructure, options.collection.schema.primaryPath, rows);
          return conflicts;
        }, () => replicationState.notifyPeers().catch(() => {}));
      },
      batchSize: options.push.batchSize,
      modifier: options.push.modifier
    };
  }
  replicationState = new RxGoogleDriveReplicationState(googleDriveOptionsWithDefaults, driveStructure, options.replicationIdentifier, collection, replicationPrimitivesPull, replicationPrimitivesPush, options.signalingOptions, options.live, options.retryTime, options.autoStart);

  /**
   * Google drive has no websocket or server-send-events
   * to observe file changes. Therefore we use WebRTC to
   * connect clients which then can ping each other on changes.
   * Instead of a signaling server, we use the google-drive itself
   * to exchange signaling data.
   */
  if (options.live && options.pull) {
    ensureProcessNextTickIsSet();
    var startBefore = replicationState.start.bind(replicationState);
    var cancelBefore = replicationState.cancel.bind(replicationState);
    replicationState.start = () => {
      replicationState.signalingState = new SignalingState(replicationState.googleDrive, replicationState.driveStructure, options.signalingOptions ? options.signalingOptions : {});
      var sub = replicationState.signalingState.resync$.subscribe(() => {
        replicationState.reSync();
      });
      replicationState.cancel = () => {
        sub.unsubscribe();
        replicationState.signalingState?.close();
        return cancelBefore();
      };
      return startBefore();
    };
  }
  startReplicationOnLeaderShip(options.waitForLeadership, replicationState);
  return replicationState;
}
//# sourceMappingURL=index.js.map