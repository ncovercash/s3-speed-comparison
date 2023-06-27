import {
  CompleteMultipartUploadCommand,
  CreateBucketCommand,
  CreateMultipartUploadCommand,
  DeleteBucketCommand,
  DeleteObjectsCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  S3Client,
  UploadPartCommand,
} from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import crypto from 'crypto';
import { writeFile } from 'fs';
import ky from 'ky-universal';
import { promisify } from 'util';

const s3 = new S3Client({
  credentials: {
    accessKeyId: process.env.ACCESS_KEY ?? 'minioadmin',
    secretAccessKey: process.env.SECRET_KEY ?? 'minioadmin',
  },
  endpoint: process.env.ENDPOINT ?? 'http://localhost:9000',
  forcePathStyle: true,
  region: 'us-east-1',
});

const Bucket = `benchmark-${crypto.randomBytes(5).toString('hex')}`;

function getFileOfSize(sizeBytes: number): Buffer {
  // use unsafe since we don't care about contents
  // mostly want to ensure this does not impact our speed
  return Buffer.allocUnsafe(sizeBytes);
}

function getKey(base: string): string {
  return `${base}/${process.hrtime()[0]}${process.hrtime()[1]}`;
}

function getTraditionalPresignedUrl(prefix: string) {
  return getSignedUrl(
    s3,
    new PutObjectCommand({
      Bucket,
      Key: getKey(prefix),
    })
  );
}

function initiateMultipart(prefix: string) {
  return s3.send(
    new CreateMultipartUploadCommand({
      Bucket,
      Key: getKey(prefix),
    })
  );
}

function getMultipartPresignedUrl(
  Key: string,
  UploadId: string,
  PartNumber: number
) {
  return getSignedUrl(
    s3,
    new UploadPartCommand({
      Bucket,
      Key,
      UploadId,
      PartNumber,
    })
  );
}

async function completeMultipart(
  UploadId: string,
  Key: string,
  eTags: string[]
) {
  await s3.send(
    new CompleteMultipartUploadCommand({
      Bucket,
      Key,
      UploadId,
      MultipartUpload: {
        Parts: eTags.map((ETag, index) => ({
          ETag,
          PartNumber: index + 1,
        })),
      },
    })
  );
}

async function traditionalUpload(prefix: string, file: Buffer) {
  const url = await getTraditionalPresignedUrl(prefix);

  await ky.put(url, { body: file });
}

async function multipartUpload(
  UploadId: string,
  Key: string,
  partNumber: number,
  file: Buffer
): Promise<string> {
  const url = await getMultipartPresignedUrl(Key, UploadId, partNumber);

  return (await ky.put(url, { body: file })).headers.get('ETag') ?? '';
}

function sizeToBytes(size: string): number {
  if (size.endsWith('k')) {
    return parseFloat(size) * 1024;
  } else if (size.endsWith('m')) {
    return parseFloat(size) * 1024 * 1024;
  } else if (size.endsWith('g')) {
    return parseFloat(size) * 1024 * 1024 * 1024;
  }
  throw new Error(`Invalid size ${size}`);
}

const measures: Record<string, number[]> = {};

async function cleanup() {
  let Objects =
    (
      await s3.send(
        new ListObjectsV2Command({
          Bucket,
          MaxKeys: 2147483647,
        })
      )
    ).Contents?.map(({ Key }) => ({ Key })) ?? [];
  while (Objects.length) {
    await s3.send(
      new DeleteObjectsCommand({
        Bucket,
        Delete: {
          Objects,
        },
      })
    );
    Objects =
      (
        await s3.send(
          new ListObjectsV2Command({
            Bucket,
            MaxKeys: 2147483647,
          })
        )
      ).Contents?.map(({ Key }) => ({ Key })) ?? [];
  }
}

async function measure<T>(
  name: string,
  run:
    | {
        setup: () => T;
        fn: (res: Awaited<T>) => Promise<unknown> | unknown;
      }
    | {
        fn: () => Promise<unknown> | unknown;
      },
  maxTimeMs: number
) {
  measures[name] = measures[name] ?? [];

  let overallStart = Date.now();

  process.stdout.write(`${name}: starting`);

  let res: Awaited<T> | undefined = undefined;
  if ('setup' in run) {
    res = await run.setup();
  }

  while (measures[name].length < 3 || Date.now() - overallStart < maxTimeMs) {
    const start = Date.now();
    if ('setup' in run) {
      await run.fn(res as Awaited<T>);
    } else {
      await run.fn();
    }
    const end = Date.now();

    measures[name].push(end - start);
    process.stdout.clearLine(0);
    process.stdout.cursorTo(0);
    process.stdout.write(
      `${name}: ${measures[name].length} samples, last ${end - start}ms`
    );
  }

  process.stdout.clearLine(0);
  process.stdout.cursorTo(0);
  console.log(
    `${name}: approx ${(
      measures[name].reduce((a, b) => a + b, 0) / measures[name].length
    ).toFixed(2)}ms (${measures[name].length} samples)`
  );

  await cleanup();
  await promisify(writeFile)('results.json', JSON.stringify(measures));
}

async function measureTraditionalUploadCases(sizes: string[]) {
  for (const size of sizes) {
    await measure(
      `traditional-upload-${size}`,
      {
        setup: () => getFileOfSize(sizeToBytes(size)),
        fn: (file) => traditionalUpload(`traditional-upload-${size}`, file),
      },
      2000
    );
  }
}

async function measureMultipartUploadCases(sizes: string[], chunks: string[]) {
  for (const size of sizes) {
    for (const chunk of chunks) {
      await measure(
        `upload-multipart-total-${size}-${chunk}`,
        {
          setup: () => {
            const chunkData = getFileOfSize(sizeToBytes(chunk));
            const lastChunkData = getFileOfSize(
              sizeToBytes(size) % sizeToBytes(chunk)
            );

            const uploadMeasure = `upload-multipart-upload-${size}-${chunk}`;
            const completeMeasure = `upload-multipart-complete-${size}-${chunk}`;
            measures[uploadMeasure] = [];
            measures[completeMeasure] = [];
            return { chunkData, lastChunkData, uploadMeasure, completeMeasure };
          },
          fn: async ({
            chunkData,
            lastChunkData,
            uploadMeasure,
            completeMeasure,
          }) => {
            let remainingBytes = sizeToBytes(size);
            const chunkSize = sizeToBytes(chunk);

            const { UploadId, Key } = await initiateMultipart(
              `multipart-upload-${size}-${chunk}`
            );

            if (!UploadId || !Key) {
              throw new Error('UploadId or Key is undefined');
            }

            const eTags: string[] = [];
            let partNumber = 1;

            let start = Date.now();
            while (remainingBytes > chunkSize) {
              eTags.push(
                await multipartUpload(UploadId, Key, partNumber, chunkData)
              );

              partNumber++;
              remainingBytes -= sizeToBytes(chunk);
            }

            if (remainingBytes > 0) {
              eTags.push(
                await multipartUpload(UploadId, Key, partNumber, lastChunkData)
              );
            }
            let end = Date.now();
            measures[uploadMeasure].push(end - start);

            start = Date.now();
            await completeMultipart(UploadId, Key, eTags);
            end = Date.now();

            measures[completeMeasure].push(end - start);
          },
        },
        sizeToBytes(size) >= sizeToBytes('1g') ? 10000 : 2000
      );
    }
  }
}

async function run() {
  console.log(`Creating bucket '${Bucket}'`);

  await s3.send(new CreateBucketCommand({ Bucket }));

  await measure(
    'get-traditional-presigned-url',
    { fn: () => getTraditionalPresignedUrl('traditional-presigned-url-only') },
    1000
  );
  await measure(
    'initiate-multipart-only',
    { fn: () => initiateMultipart('initiate-multipart-only') },
    1000
  );
  await measure(
    'get-multipart-presigned-url-only',
    {
      setup: async () => {
        const { Key, UploadId } = await initiateMultipart(
          'multipart-presigned-url-only'
        );
        if (!Key || !UploadId) {
          throw new Error('Key or UploadId is undefined');
        }
        return { Key, UploadId };
      },
      fn: ({ Key, UploadId }) => getMultipartPresignedUrl(Key, UploadId, 1),
    },
    1000
  );
  await measureTraditionalUploadCases([
    '512k',
    '5m',
    '10m',
    '25m',
    '50m',
    '100m',
    '1g',
    '1.5g',
    // got OS/syscall errors attempting 2g :(
  ]);
  await measureMultipartUploadCases(
    ['512k', '5m', '10m', '25m', '50m', '100m'],
    ['5m', '25m', '50m', '100m']
  );
  await measureMultipartUploadCases(
    ['1g', '2g', '5g'],
    ['5m', '25m', '50m', '100m', '200m', '500m']
  );

  console.log('Deleting temporary S3 files...');
  await cleanup();

  console.log(`Deleting bucket '${Bucket}'`);
  await s3.send(new DeleteBucketCommand({ Bucket }));
}

run();
