export function defaultsHelper<T>(input: T, defaultInput: T): T {
    if (!input) {
        return defaultInput;
    }

    for (let key in Object.keys(defaultInput)) {
        input[key] = input[key] || defaultInput[key];
    }

    return input;
}