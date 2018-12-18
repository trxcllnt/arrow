module.exports = {
    ...require('./jest.config'),
    "globals": {
        "ts-jest": {
            "diagnostics": false,
            "tsConfig": "test/tsconfig.coverage.json"
        }
    }
};
