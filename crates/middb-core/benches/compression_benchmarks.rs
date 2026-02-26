use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use middb_core::compression::{compress, decompress, CompressionType};

fn generate_data(size: usize, compressibility: f64) -> Vec<u8> {
    let mut data = Vec::with_capacity(size);
    let repeat_len = (size as f64 * compressibility) as usize;
    let random_len = size - repeat_len;
    // Repeating pattern (compressible)
    for i in 0..repeat_len {
        data.push((i % 256) as u8);
    }
    // Pseudo-random data (less compressible)
    let mut state = 0x12345678u32;
    for _ in 0..random_len {
        state = state.wrapping_mul(1103515245).wrapping_add(12345);
        data.push((state >> 16) as u8);
    }
    data
}

fn bench_compress(c: &mut Criterion) {
    let mut group = c.benchmark_group("compress");
    let data = generate_data(64 * 1024, 0.7); // 64KB, 70% compressible

    for ct in [CompressionType::Lz4, CompressionType::Snappy] {
        let name = format!("{:?}", ct);
        group.throughput(Throughput::Bytes(data.len() as u64));
        group.bench_with_input(BenchmarkId::new("64KB", &name), &ct, |b, ct| {
            b.iter(|| compress(&data, *ct));
        });
    }
    group.finish();
}

fn bench_decompress(c: &mut Criterion) {
    let mut group = c.benchmark_group("decompress");
    let data = generate_data(64 * 1024, 0.7);

    for ct in [CompressionType::Lz4, CompressionType::Snappy] {
        let name = format!("{:?}", ct);
        let compressed = compress(&data, ct);
        group.throughput(Throughput::Bytes(data.len() as u64));
        group.bench_with_input(BenchmarkId::new("64KB", &name), &compressed, |b, compressed| {
            b.iter(|| decompress(compressed).unwrap());
        });
    }
    group.finish();
}

fn bench_compression_ratio(c: &mut Criterion) {
    let mut group = c.benchmark_group("compression_ratio");
    group.sample_size(10);

    for (label, compressibility) in [("high", 0.9), ("medium", 0.5), ("low", 0.1)] {
        let data = generate_data(256 * 1024, compressibility);
        for ct in [CompressionType::Lz4, CompressionType::Snappy] {
            let name = format!("{:?}_{}", ct, label);
            group.bench_with_input(BenchmarkId::new("256KB", &name), &(&data, ct), |b, (data, ct)| {
                b.iter(|| {
                    let compressed = compress(data, *ct);
                    compressed.len()
                });
            });
        }
    }
    group.finish();
}

fn bench_end_to_end(c: &mut Criterion) {
    let mut group = c.benchmark_group("compress_decompress_roundtrip");
    let data = generate_data(64 * 1024, 0.7);

    for ct in [CompressionType::None, CompressionType::Lz4, CompressionType::Snappy] {
        let name = format!("{:?}", ct);
        group.throughput(Throughput::Bytes(data.len() as u64));
        group.bench_with_input(BenchmarkId::new("64KB", &name), &ct, |b, ct| {
            b.iter(|| {
                let compressed = compress(&data, *ct);
                decompress(&compressed).unwrap()
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_compress, bench_decompress, bench_compression_ratio, bench_end_to_end);
criterion_main!(benches);
