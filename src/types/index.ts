export interface RawBatchItem {
  batch_id: string;
  id: string;
  position: number[];
  timestamp: number;
  sensors?: any;
}

export interface LocationBucketData {
  batchId: string;
  startTime: Date;
  endTime: Date;
  count: number;
  data: any;
}
