export interface WalkObjectCallbackArgs {
    value: unknown;
    key: string;
    location: (string | number)[];
    isLeaf: boolean;
}

export interface WalkObjectCallback {
    (obj: WalkObjectCallbackArgs): void;
}

// eslint-disable-next-line @typescript-eslint/no-empty-object-type
export interface UnknownObject extends Record<string | number | symbol, unknown> {}

export const isObject = (item: unknown) => {
    return typeof item === 'object' && !Array.isArray(item) && item !== null;
};

export const walkObject = (root: UnknownObject, fn: WalkObjectCallback) => {
    const walk = (obj: UnknownObject, location: (string | number)[] = []) => {
        Object.keys(obj).forEach((key) => {
            // Value is an array, call walk on each item in the array
            if (Array.isArray(obj[key])) {
                obj[key].forEach((el, j) => {
                    fn({
                        value: el,
                        key: `${key}:${j}`,
                        location: [...location, ...[key], ...[j]],
                        isLeaf: false
                    });

                    walk(el, [...location, ...[key], ...[j]]);
                });

                // Value is an object, walk the keys of the object
            } else if (isObject(obj[key])) {
                const el = obj[key] as UnknownObject;
                fn({
                    value: obj[key],
                    key,
                    location: [...location, ...[key]],
                    isLeaf: false
                });
                walk(el, [...location, ...[key]]);

                // We've reached a leaf node, call fn on the leaf with the location
            } else {
                fn({
                    value: obj[key],
                    key,
                    location: [...location, ...[key]],
                    isLeaf: true
                });
            }
        });
    };

    walk(root);
};
