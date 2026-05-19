import { RxReplicationState } from '../replication/index.ts';
import { WebsocketClientOptions } from './websocket-types.ts';
import { Subject, BehaviorSubject } from 'rxjs';
import type { RxError } from '../../types/index.d.ts';
export type WebsocketClient = {
    url: string;
    socket: any;
    connected$: BehaviorSubject<boolean>;
    message$: Subject<any>;
    error$: Subject<RxError>;
};
export declare function createWebSocketClient<RxDocType>(options: WebsocketClientOptions<RxDocType>): Promise<WebsocketClient>;
export declare function replicateWithWebsocketServer<RxDocType, CheckpointType>(options: WebsocketClientOptions<RxDocType>): Promise<RxReplicationState<RxDocType, CheckpointType>>;
