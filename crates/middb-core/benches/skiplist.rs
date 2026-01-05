use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use middb_core::SkipList;
use std::collections::BTreeMap;

fn skiplist_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert");

    for size in [100, 1000, 10000] {
        group.bench_with_input(BenchmarkId::new("SkipList", size), &size, |b, &size| {
            b.iter(|| {
                let mut list = SkipList::new();
                for i in 0..size {
                    list.insert(black_box(i), black_box(i * 2));
                }
            });
        });

        group.bench_with_input(BenchmarkId::new("BTreeMap", size), &size, |b, &size| {
            b.iter(|| {
                let mut map = BTreeMap::new();
                for i in 0..size {
                    map.insert(black_box(i), black_box(i * 2));
                }
            });
        });
    }

    group.finish();
}

fn skiplist_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("get");

    for size in [100, 1000, 10000] {
        // Setup SkipList
        let mut list = SkipList::new();
        for i in 0..size {
            list.insert(i, i * 2);
        }

        group.bench_with_input(BenchmarkId::new("SkipList", size), &size, |b, &size| {
            b.iter(|| {
                for i in 0..size {
                    black_box(list.get(&i));
                }
            });
        });

        // Setup BTreeMap
        let mut map = BTreeMap::new();
        for i in 0..size {
            map.insert(i, i * 2);
        }

        group.bench_with_input(BenchmarkId::new("BTreeMap", size), &size, |b, &size| {
            b.iter(|| {
                for i in 0..size {
                    black_box(map.get(&i));
                }
            });
        });
    }

    group.finish();
}

fn skiplist_iter(c: &mut Criterion) {
    let mut group = c.benchmark_group("iterate");

    for size in [100, 1000, 10000] {
        let mut list = SkipList::new();
        for i in 0..size {
            list.insert(i, i * 2);
        }

        group.bench_with_input(BenchmarkId::new("SkipList", size), &size, |b, _| {
            b.iter(|| {
                for item in list.iter() {
                    black_box(item);
                }
            });
        });

        let mut map = BTreeMap::new();
        for i in 0..size {
            map.insert(i, i * 2);
        }

        group.bench_with_input(BenchmarkId::new("BTreeMap", size), &size, |b, _| {
            b.iter(|| {
                for item in map.iter() {
                    black_box(item);
                }
            });
        });
    }

    group.finish();
}

criterion_group!(benches, skiplist_insert, skiplist_get, skiplist_iter);
criterion_main!(benches);
