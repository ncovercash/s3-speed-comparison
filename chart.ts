import { ChartConfiguration, ChartOptions } from 'chart.js';
import ChartJSImage from 'chartjs-to-image';
import { Stats } from 'fast-stats';
import { mkdirSync, readFileSync } from 'fs';
import { writeFile } from 'fs/promises';
import { resolve } from 'path';
import { table } from 'table';

type TraditionalTestSize =
  | '512k'
  | '5m'
  | '10m'
  | '25m'
  | '50m'
  | '100m'
  | '1g'
  | '1.5g';
type MultipartTestSize = '512k' | '5m' | '10m' | '25m' | '50m' | '100m';
type MultipartBigTestSize = '1g' | '2g' | '5g';
type MultipartChunkSize = '5m' | '25m' | '50m' | '100m';
type MultipartBigChunkSize = MultipartChunkSize | '200m' | '500m';

type Key =
  | 'get-traditional-presigned-url'
  | 'get-multipart-presigned-url-only'
  | 'initiate-multipart-only'
  | `traditional-upload-${TraditionalTestSize}`
  | `upload-multipart-total-${MultipartTestSize}-${MultipartChunkSize}`
  | `upload-multipart-upload-${MultipartTestSize}-${MultipartChunkSize}`
  | `upload-multipart-complete-${MultipartTestSize}-${MultipartChunkSize}`
  | `upload-multipart-total-${MultipartBigTestSize}-${MultipartBigChunkSize}`
  | `upload-multipart-upload-${MultipartBigTestSize}-${MultipartBigChunkSize}`
  | `upload-multipart-complete-${MultipartBigTestSize}-${MultipartBigChunkSize}`;

const basicTableConfig = {
  drawHorizontalLine: (index: number, size: number) =>
    index === 0 || index === 1 || index === size,
};
const outputTableConfig = {
  border: {
    topBody: '',
    topJoin: '',
    topLeft: '',
    topRight: '',
    bottomBody: '',
    bottomJoin: '',
    bottomLeft: '',
    bottomRight: '',
    bodyLeft: '|`',
    bodyRight: '`|',
    bodyJoin: '`|`',
    joinBody: '-',
    joinLeft: '|',
    joinRight: '|',
    joinJoin: '|',
  },
  drawHorizontalLine: (index: number) => index === 1,
};

const chart = new ChartJSImage();
chart.setChartJsVersion('4.3.0');
chart.setWidth(800);
chart.setHeight(600);

const results = JSON.parse(readFileSync(process.argv[2], 'utf8')) as Record<
  Key,
  number[]
>;

const dir = resolve(process.cwd(), 'results');
console.log(`Making results directory ${dir}`);
mkdirSync(dir, { recursive: true });

async function renderChartToFile(options: ChartConfiguration, name: string) {
  chart.setConfig(options);
  console.log(`Rendering ${name}`);
  await chart.toFile(resolve(dir, name));
}

async function processBasicStats() {
  console.log('Presigned URL statistics:');

  const values = {
    presignedTraditional: new Stats().push(
      results['get-traditional-presigned-url']
    ),
    presignedMultipart: new Stats().push(
      results['get-multipart-presigned-url-only']
    ),
    multipartInitiate: new Stats().push(results['initiate-multipart-only']),
  };

  const data = [
    ['Test name', 'Mean (ms)', '± (ms)'],
    [
      'Get traditional presigned URL',
      values.presignedTraditional.amean().toFixed(3),
      values.presignedTraditional.stddev().toFixed(3),
    ],
    [
      'Get multipart presigned URL',
      values.presignedMultipart.amean().toFixed(3),
      values.presignedMultipart.stddev().toFixed(3),
    ],
    [
      'Initiate multipart upload',
      values.multipartInitiate.amean().toFixed(3),
      values.multipartInitiate.stddev().toFixed(3),
    ],
  ];

  console.log(
    table(data, {
      ...basicTableConfig,
      columns: [
        { alignment: 'left' },
        { alignment: 'right' },
        { alignment: 'right' },
      ],
    })
  );
  await writeFile(
    resolve(dir, 'basic-stats.md'),
    table(data, outputTableConfig)
  );
}

async function fullUploadStats() {
  console.log('Upload type statistics:');

  const byType: Record<
    'traditional' | `multipart-${MultipartBigChunkSize}-chunks`,
    Partial<
      Record<
        TraditionalTestSize | MultipartTestSize | MultipartBigTestSize,
        number
      >
    >
  > = {
    traditional: {},
    'multipart-5m-chunks': {},
    'multipart-25m-chunks': {},
    'multipart-50m-chunks': {},
    'multipart-100m-chunks': {},
    'multipart-200m-chunks': {},
    'multipart-500m-chunks': {},
  };

  for (const key in results) {
    if (key.startsWith('traditional-upload')) {
      const size = key.split('-')[2] as TraditionalTestSize;
      byType.traditional[size] = new Stats().push(results[key as Key]).amean();
    }
    if (key.startsWith('upload-multipart-total')) {
      const size = key.split('-')[3] as
        | MultipartTestSize
        | MultipartBigTestSize;
      const chunkSize = key.split('-')[4] as MultipartBigChunkSize;
      byType[`multipart-${chunkSize}-chunks`][size] = new Stats()
        .push(...results[key as Key])
        .amean();
    }
  }

  function dataFromRow(
    row: Partial<
      Record<
        TraditionalTestSize | MultipartTestSize | MultipartBigTestSize,
        number
      >
    >
  ) {
    return [
      row['512k'],
      row['5m'],
      row['10m'],
      row['25m'],
      row['50m'],
      row['100m'],
      row['1g'],
      row['1.5g'],
      row['2g'],
      row['5g'],
    ].map((v) => v ?? null);
  }

  function tableFromRow(
    row: Partial<
      Record<
        TraditionalTestSize | MultipartTestSize | MultipartBigTestSize,
        number
      >
    >
  ) {
    return dataFromRow(row).map((v) => v?.toFixed(3) ?? '');
  }

  const tableValues = [
    [
      'Upload type',
      '512k',
      '5m',
      '10m',
      '25m',
      '50m',
      '100m',
      '1g',
      '1.5g',
      '2g',
      '5g',
    ],
    ['Traditional', ...tableFromRow(byType.traditional)],
    ['Multipart (5m chunks)', ...tableFromRow(byType['multipart-5m-chunks'])],
    ['Multipart (25m chunks)', ...tableFromRow(byType['multipart-25m-chunks'])],
    ['Multipart (50m chunks)', ...tableFromRow(byType['multipart-50m-chunks'])],
    [
      'Multipart (100m chunks)',
      ...tableFromRow(byType['multipart-100m-chunks']),
    ],
    [
      'Multipart (200m chunks)',
      ...tableFromRow(byType['multipart-200m-chunks']),
    ],
    [
      'Multipart (500m chunks)',
      ...tableFromRow(byType['multipart-500m-chunks']),
    ],
  ];

  console.log(
    table(tableValues, {
      ...basicTableConfig,
      columns: [
        { alignment: 'left' },
        ...Array(10).fill({ alignment: 'right' }),
      ],
    })
  );
  await renderChartToFile(
    {
      type: 'line',
      data: {
        labels: [
          '512k',
          '5m',
          '10m',
          '25m',
          '50m',
          '100m',
          '1g',
          '1.5g',
          '2g',
          '5g',
        ],
        datasets: [
          {
            label: 'Traditional',
            data: dataFromRow(byType.traditional),
            spanGaps: true,
            borderColor: 'black',
            borderWidth: 5,
          },
          {
            label: 'Multipart (5m chunks)',
            data: dataFromRow(byType['multipart-5m-chunks']),
            spanGaps: true,
            borderColor: 'red',
          },
          {
            label: 'Multipart (25m chunks)',
            data: dataFromRow(byType['multipart-25m-chunks']),
            spanGaps: true,
            borderColor: 'orange',
          },
          {
            label: 'Multipart (50m chunks)',
            data: dataFromRow(byType['multipart-50m-chunks']),
            spanGaps: true,
            borderColor: 'yellow',
          },
          {
            label: 'Multipart (100m chunks)',
            data: dataFromRow(byType['multipart-100m-chunks']),
            spanGaps: true,
            borderColor: 'green',
          },
          {
            label: 'Multipart (200m chunks)',
            data: dataFromRow(byType['multipart-200m-chunks']),
            spanGaps: true,
            borderColor: 'blue',
          },
          {
            label: 'Multipart (500m chunks)',
            data: dataFromRow(byType['multipart-500m-chunks']),
            spanGaps: true,
            borderColor: 'purple',
          },
        ],
      },
      options: {
        plugins: {
          title: {
            display: true,
            text: 'Upload Time By Upload Type',
          },
        },
        scales: {
          x: {
            display: true,
            title: { display: true, text: 'File size' },
          },
          y: {
            display: true,
            type: 'logarithmic',
            title: { display: true, text: 'Upload time (ms, lower is better)' },
          },
        },
      },
    },
    'upload-stats.png'
  );

  await renderChartToFile(
    {
      type: 'line',
      data: {
        labels: ['512k', '5m', '10m', '25m', '50m', '100m'],
        datasets: [
          {
            label: 'Traditional',
            data: dataFromRow(byType.traditional).slice(0, 6),
            spanGaps: true,
            borderColor: 'black',
            borderWidth: 5,
          },
          {
            label: 'Multipart (5m chunks)',
            data: dataFromRow(byType['multipart-5m-chunks']).slice(0, 6),
            spanGaps: true,
            borderColor: 'red',
          },
          {
            label: 'Multipart (25m chunks)',
            data: dataFromRow(byType['multipart-25m-chunks']).slice(0, 6),
            spanGaps: true,
            borderColor: 'orange',
          },
          {
            label: 'Multipart (50m chunks)',
            data: dataFromRow(byType['multipart-50m-chunks']).slice(0, 6),
            spanGaps: true,
            borderColor: 'yellow',
          },
          {
            label: 'Multipart (100m chunks)',
            data: dataFromRow(byType['multipart-100m-chunks']).slice(0, 6),
            spanGaps: true,
            borderColor: 'green',
          },
        ],
      },
      options: {
        plugins: {
          title: {
            display: true,
            text: 'Upload Time By Upload Type (Truncated)',
          },
        },
        scales: {
          x: {
            display: true,
            title: { display: true, text: 'File size' },
          },
          y: {
            display: true,
            type: 'logarithmic',
            title: { display: true, text: 'Upload time (ms, lower is better)' },
          },
        },
      },
    },
    'upload-stats-detailed.png'
  );

  await writeFile(
    resolve(dir, 'upload-stats.md'),
    table(tableValues, outputTableConfig) +
      '\n\n' +
      '*Times are in milliseconds, lower is better.*' +
      '\n\n' +
      '![Chart](upload-stats.png)' +
      '\n\n' +
      '![Chart](upload-stats-detailed.png)'
  );
}

async function processDetailedMultipartStats() {
  function dataFromRow(
    chunkSize: MultipartBigChunkSize,
    type: 'upload' | 'complete'
  ) {
    return [
      new Stats()
        .push(results[`upload-multipart-${type}-512k-${chunkSize}` as Key])
        .amean(),
      new Stats()
        .push(results[`upload-multipart-${type}-5m-${chunkSize}` as Key])
        .amean(),
      new Stats()
        .push(results[`upload-multipart-${type}-10m-${chunkSize}` as Key])
        .amean(),
      new Stats()
        .push(results[`upload-multipart-${type}-25m-${chunkSize}` as Key])
        .amean(),
      new Stats()
        .push(results[`upload-multipart-${type}-50m-${chunkSize}` as Key])
        .amean(),
      new Stats()
        .push(results[`upload-multipart-${type}-100m-${chunkSize}` as Key])
        .amean(),
      new Stats()
        .push(results[`upload-multipart-${type}-1g-${chunkSize}` as Key])
        .amean(),
      new Stats()
        .push(results[`upload-multipart-${type}-2g-${chunkSize}` as Key])
        .amean(),
      new Stats()
        .push(results[`upload-multipart-${type}-5g-${chunkSize}` as Key])
        .amean(),
    ];
  }

  await renderChartToFile(
    {
      type: 'bar',
      data: {
        labels: ['512k', '5m', '10m', '25m', '50m', '100m', '1g', '2g', '5g'],
        datasets: [
          {
            label: '5m chunks upload time',
            data: dataFromRow('5m', 'upload'),
            backgroundColor: 'red',
            stack: '5m chunks',
          },
          {
            label: '5m chunks reconcile time',
            data: dataFromRow('5m', 'complete'),
            backgroundColor: 'white',
            borderWidth: 2,
            borderColor: 'red',
            stack: '5m chunks',
          },
          {
            label: '25m chunks upload time',
            data: dataFromRow('25m', 'upload'),
            backgroundColor: 'orange',
            stack: '25m chunks',
          },
          {
            label: '25m chunks reconcile time',
            data: dataFromRow('25m', 'complete'),
            backgroundColor: 'white',
            borderWidth: 2,
            borderColor: 'orange',
            stack: '25m chunks',
          },
          {
            label: '50m chunks upload time',
            data: dataFromRow('50m', 'upload'),
            backgroundColor: 'yellow',
            stack: '50m chunks',
          },
          {
            label: '50m chunks reconcile time',
            data: dataFromRow('50m', 'complete'),
            backgroundColor: 'white',
            borderWidth: 2,
            borderColor: 'yellow',
            stack: '50m chunks',
          },
          {
            label: '100m chunks upload time',
            data: dataFromRow('100m', 'upload'),
            backgroundColor: 'green',
            stack: '100m chunks',
          },
          {
            label: '100m chunks reconcile time',
            data: dataFromRow('100m', 'complete'),
            backgroundColor: 'white',
            borderWidth: 2,
            borderColor: 'green',
            stack: '100m chunks',
          },
          {
            label: '200m chunks upload time',
            data: dataFromRow('200m', 'upload'),
            backgroundColor: 'blue',
            stack: '200m chunks',
          },
          {
            label: '200m chunks reconcile time',
            data: dataFromRow('200m', 'complete'),
            backgroundColor: 'white',
            borderWidth: 2,
            borderColor: 'blue',
            stack: '200m chunks',
          },
          {
            label: '500m chunks upload time',
            data: dataFromRow('500m', 'upload'),
            backgroundColor: 'purple',
            stack: '500m chunks',
          },
          {
            label: '500m chunks reconcile time',
            data: dataFromRow('500m', 'complete'),
            backgroundColor: 'white',
            borderWidth: 2,
            borderColor: 'purple',
            stack: '500m chunks',
          },
        ],
      },
      options: {
        plugins: {
          title: {
            display: true,
            text: 'Upload Time By Chunk Size',
          },
        },
        scales: {
          x: {
            display: true,
            title: { display: true, text: 'File size' },
            stacked: true,
          },
          y: {
            display: true,
            type: 'logarithmic',
            title: { display: true, text: 'Upload time (ms, lower is better)' },
            stacked: true,
          },
        },
      },
    },
    'multipart-chunk-size-comparison.png'
  );

  const tableData = [
    [
      'Chunk size',
      'Stage',
      '512k',
      '5m',
      '10m',
      '25m',
      '50m',
      '100m',
      '1g',
      '2g',
      '5g',
    ],
    [
      '5m',
      'Upload',
      ...dataFromRow('5m', 'upload').map((x) => (isNaN(x) ? '' : x.toFixed(3))),
    ],
    [
      '5m',
      'Reconcile',
      ...dataFromRow('5m', 'complete').map((x) =>
        isNaN(x) ? '' : x.toFixed(3)
      ),
    ],
    [
      '25m',
      'Upload',
      ...dataFromRow('25m', 'upload').map((x) =>
        isNaN(x) ? '' : x.toFixed(3)
      ),
    ],
    [
      '25m',
      'Reconcile',
      ...dataFromRow('25m', 'complete').map((x) =>
        isNaN(x) ? '' : x.toFixed(3)
      ),
    ],
    [
      '50m',
      'Upload',
      ...dataFromRow('50m', 'upload').map((x) =>
        isNaN(x) ? '' : x.toFixed(3)
      ),
    ],
    [
      '50m',
      'Reconcile',
      ...dataFromRow('50m', 'complete').map((x) =>
        isNaN(x) ? '' : x.toFixed(3)
      ),
    ],
    [
      '100m',
      'Upload',
      ...dataFromRow('100m', 'upload').map((x) =>
        isNaN(x) ? '' : x.toFixed(3)
      ),
    ],
    [
      '100m',
      'Reconcile',
      ...dataFromRow('100m', 'complete').map((x) =>
        isNaN(x) ? '' : x.toFixed(3)
      ),
    ],
    [
      '200m',
      'Upload',
      ...dataFromRow('200m', 'upload').map((x) =>
        isNaN(x) ? '' : x.toFixed(3)
      ),
    ],
    [
      '200m',
      'Reconcile',
      ...dataFromRow('200m', 'complete').map((x) =>
        isNaN(x) ? '' : x.toFixed(3)
      ),
    ],
    [
      '500m',
      'Upload',
      ...dataFromRow('500m', 'upload').map((x) =>
        isNaN(x) ? '' : x.toFixed(3)
      ),
    ],
    [
      '500m',
      'Reconcile',
      ...dataFromRow('500m', 'complete').map((x) =>
        isNaN(x) ? '' : x.toFixed(3)
      ),
    ],
  ];

  console.log('Upload vs reconcile times by chunk size:');
  console.log(
    table(tableData, {
      ...basicTableConfig,
      columns: [
        { alignment: 'left' },
        { alignment: 'left' },
        ...Array(11).fill({ alignment: 'right' }),
      ],
    })
  );

  await writeFile(
    resolve(dir, 'multipart-chunk-size-comparison.md'),
    table(tableData, {
      ...outputTableConfig,
      columns: [
        { alignment: 'left' },
        { alignment: 'left' },
        ...Array(11).fill({ alignment: 'right' }),
      ],
    }) +
      '\n\n' +
      '*Times are in milliseconds, lower is better.*' +
      '\n\n' +
      '![Chart](multipart-chunk-size-comparison.png)'
  );
}

async function run() {
  await processBasicStats();
  await fullUploadStats();
  await processDetailedMultipartStats();
}

run();
