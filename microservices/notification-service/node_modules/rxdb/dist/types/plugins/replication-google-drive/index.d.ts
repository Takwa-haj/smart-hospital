import type { RxCollection, ReplicationPullOptions, ReplicationPushOptions } from '../../types/index.d.ts';
import { RxReplicationState } from '../replication/index.ts';
import type { GoogleDriveCheckpointType, GoogleDriveOptionsWithDefaults, SyncOptionsGoogleDrive } from './google-drive-types.ts';
import { DriveStructure } from './init.ts';
import { SignalingOptions, SignalingState } from './signaling.ts';
export * from './google-drive-types.ts';
export * from './google-drive-helper.ts';
export * from './transaction.ts';
export * from './init.ts';
export * from './document-handling.ts';
export * from './multipart.ts';
export * from './downstream.ts';
export * from './upstream.ts';
export * from './signaling.ts';
export declare const DEFAULT_TRANSACTION_TIMEOUT: number;
export declare class RxGoogleDriveReplicationState<RxDocType> extends RxReplicationState<RxDocType, GoogleDriveCheckpointType> {
    readonly googleDrive: GoogleDriveOptionsWithDefaults;
    readonly driveStructure: DriveStructure;
    readonly replicationIdentifierHash: string;
    readonly collection: RxCollection<RxDocType, any>;
    readonly pull?: ReplicationPullOptions<RxDocType, GoogleDriveCheckpointType> | undefined;
    readonly push?: ReplicationPushOptions<RxDocType> | undefined;
    readonly signalingOptions?: SignalingOptions | undefined;
    readonly live: boolean;
    retryTime: number;
    autoStart: boolean;
    /**
     * Only exists on live replication
     */
    signalingState?: SignalingState;
    constructor(googleDrive: GoogleDriveOptionsWithDefaults, driveStructure: DriveStructure, replicationIdentifierHash: string, collection: RxCollection<RxDocType, any>, pull?: ReplicationPullOptions<RxDocType, GoogleDriveCheckpointType> | undefined, push?: ReplicationPushOptions<RxDocType> | undefined, signalingOptions?: SignalingOptions | undefined, live?: boolean, retryTime?: number, autoStart?: boolean);
    /**
     * Notify other peers that something
     * has or might have changed so that
     * they can pull from their checkpoints.
     */
    notifyPeers(): Promise<void>;
}
export declare function replicateGoogleDrive<RxDocType>(options: SyncOptionsGoogleDrive<RxDocType>): Promise<RxGoogleDriveReplicationState<RxDocType>>;
