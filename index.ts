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
import benny from 'benny';
import crypto from 'crypto';
import ky from 'ky-universal';
import { writeFile } from 'fs';
import { promisify } from 'util';

const s3 = new S3Client({
  credentials: {
    accessKeyId: process.env.ACCESS_KEY ?? 'minioadmin',
    secretAccessKey: process.env.SECRET_KEY ?? 'minioadmin',
  },
  endpoint: 'http://localhost:9000',
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

const optionsShort = { delay: 0, maxTime: 1, initCount: 0 };
const optionsMedium = { delay: 0, maxTime: 2, initCount: 0 };
const optionsLong = { delay: 0, maxTime: 10, initCount: 0 };

function getTraditionalUploadCases(sizes: string[]) {
  return sizes.map((size) =>
    benny.add(
      `traditional upload ${size}`,
      () => {
        const file = getFileOfSize(sizeToBytes(size));
        return async () =>
          await traditionalUpload(`traditional-upload-${size}`, file);
      },
      optionsMedium
    )
  );
}

const measures: Record<string, number[]> = {};

function getMultipartUploadCases(sizes: string[], chunks: string[]) {
  return sizes.flatMap((size) =>
    chunks.map((chunk) =>
      benny.add(
        `multipart upload ${size} with ${chunk} chunks`,
        () => {
          const chunkData = getFileOfSize(sizeToBytes(chunk));
          const lastChunkData = getFileOfSize(
            sizeToBytes(size) % sizeToBytes(chunk)
          );

          const uploadMeasure = `upload-multipart-upload-${size}-${chunk}`;
          const completeMeasure = `complete-multipart-upload-${size}-${chunk}`;
          measures[uploadMeasure] = [];
          measures[completeMeasure] = [];

          return async () => {
            let remainingBytes = sizeToBytes(size);
            const chunkSize = sizeToBytes(chunk);

            let start = Date.now();
            let end = Date.now();

            const { UploadId, Key } = await initiateMultipart(
              `multipart-upload-${size}-${chunk}`
            );

            if (!UploadId || !Key) {
              throw new Error('UploadId or Key is undefined');
            }

            const eTags: string[] = [];
            let partNumber = 1;

            start = Date.now();
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
            end = Date.now();
            measures[uploadMeasure].push(end - start);

            start = Date.now();
            await completeMultipart(UploadId, Key, eTags);
            end = Date.now();

            measures[completeMeasure].push(end - start);
          };
        },
        sizeToBytes(size) >= sizeToBytes('1g') ? optionsLong : optionsShort
      )
    )
  );
}

async function run() {
  console.log(`Creating bucket '${Bucket}'`);

  await s3.send(new CreateBucketCommand({ Bucket }));

  await benny.suite(
    'Test',
    benny.add(
      'get traditional presigned URL only',
      () => getTraditionalPresignedUrl('traditional-presigned-url-only'),
      optionsShort
    ),
    benny.add(
      'initiate multipart upload only',
      () => initiateMultipart('initiate-multipart-only'),
      optionsShort
    ),
    benny.add(
      'get multipart presigned URL only',
      async () => {
        const { Key, UploadId } = await initiateMultipart(
          'multipart-presigned-url-only'
        );
        if (!Key || !UploadId) {
          throw new Error('Key or UploadId is undefined');
        }

        return () => getMultipartPresignedUrl(Key, UploadId, 1);
      },
      optionsShort
    ),

    ...getTraditionalUploadCases([
      '512k',
      '5m',
      '10m',
      '25m',
      '50m',
      '100m',
      '1g',
      '1.5g',
      // got OS/syscall errors attempting 2g :(
    ]),

    ...getMultipartUploadCases(
      ['512k', '5m', '10m', '25m', '50m', '100m', '1g', '2g', '5g'],
      ['5m', '25m', '50m', '100m']
    ),

    benny.cycle((result) =>
      console.log(
        `${result.name}: ${result.details.mean.toFixed(
          3
        )} ± ${result.details.marginOfError.toFixed(3)} seconds/op (${
          result.samples
        } samples)`
      )
    ),
    benny.complete(() => ({})),
    benny.save({ file: 'result', details: true })
  );

  console.log('Saving supplemental multipart data...');
  Object.values(measures).forEach((measure) => measure.unshift());
  await promisify(writeFile)(
    'benchmark/results/multipart-detailed.json',
    JSON.stringify(measures)
  );

  console.log('Deleting temporary files...');
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

  console.log(`Deleting bucket '${Bucket}'`);
  await s3.send(new DeleteBucketCommand({ Bucket }));
}

run();
