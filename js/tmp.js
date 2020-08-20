function searchOffsets(offsets, fn) {
    const len = offsets.length - 1;
    return function(idx) {
        let lhs = 0, mid = 0, rhs = len;
        do {
            if (lhs === rhs - 1) {
                return (offsets[rhs] < idx) ? null : fn(this, lhs, idx - offsets[lhs]);
            }
            mid = lhs + (((rhs - lhs) * .5) | 0);
            idx < offsets[mid] ? (rhs = mid) : (lhs = mid);
        } while (lhs < rhs);
        return null;
    }
}

const obj = {};

obj.get = searchOffsets([0, 10, 20, 30], (self, idx, pos) => ({ idx, pos }));
[
    { key: 0, idx: 0, pos: 0 },
    { key: 1, idx: 0, pos: 1 },
    { key: 5, idx: 0, pos: 5 },
    { key: 9, idx: 0, pos: 9 },
    { key: 10, idx: 1, pos: 0 },
    { key: 11, idx: 1, pos: 1 },
    { key: 15, idx: 1, pos: 5 },
    { key: 19, idx: 1, pos: 9 },
    { key: 20, idx: 2, pos: 0 },
    { key: 21, idx: 2, pos: 1 },
    { key: 31, idx: null, pos: null },
].forEach(validate);

obj.get = searchOffsets([0, 30], (self, idx, pos) => ({ idx, pos }));
[
    { key: 0, idx: 0, pos: 0 },
    { key: 1, idx: 0, pos: 1 },
    { key: 5, idx: 0, pos: 5 },
    { key: 9, idx: 0, pos: 9 },
    { key: 10, idx: 0, pos: 10 },
    { key: 11, idx: 0, pos: 11 },
    { key: 15, idx: 0, pos: 15 },
    { key: 19, idx: 0, pos: 19 },
    { key: 20, idx: 0, pos: 20 },
    { key: 21, idx: 0, pos: 21 },
    { key: 31, idx: null, pos: null },
].forEach(validate);

function validate({ key, ...expected }, i) {
    const actual = obj.get(key) || { idx: null, pos: null };
    if (actual.idx !== expected.idx || actual.pos !== expected.pos) {
        console.log(`${i}:`, { key, expected, actual });
    }
}
