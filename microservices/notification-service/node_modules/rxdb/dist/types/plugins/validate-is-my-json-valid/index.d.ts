import type { RxJsonSchema } from '../../types/index.d.ts';
export declare function getValidator(schema: RxJsonSchema<any>): (docData: any) => any;
export declare const wrappedValidateIsMyJsonValidStorage: <Internals, InstanceCreationOptions>(args: {
    storage: import("../../index.ts").RxStorage<Internals, InstanceCreationOptions>;
}) => import("../../index.ts").RxStorage<Internals, InstanceCreationOptions>;
