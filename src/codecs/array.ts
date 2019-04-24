/*!
 * This source file is part of the EdgeDB open source project.
 *
 * Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {ICodec, uuid} from "./ifaces";
import {WriteBuffer, ReadBuffer} from "../buffer";

const EMPTY_ARRAY = Object.freeze([]);

export class ArrayCodec implements ICodec {
  readonly tid: uuid;
  readonly tidBuffer: Buffer;
  readonly isScalar = false;

  private subCodec: ICodec;
  private len: number;

  constructor(tid: uuid, subCodec: ICodec, len: number) {
    this.tid = tid;
    this.tidBuffer = Buffer.from(tid, "hex");
    this.subCodec = subCodec;
    this.len = len;
  }

  encode(buf: WriteBuffer, obj: any): void {
    if (!this.subCodec.isScalar) {
      throw new Error("only arrays of scalars are supported");
    }

    if (!Array.isArray(obj) && !isTypedArray(obj)) {
      throw new Error("an array was expected");
    }

    const subCodec = this.subCodec;
    const elemData = new WriteBuffer();
    const objLen = obj.length;

    if (objLen > 0x7fffffff) {
      // objLen > MAXINT32
      throw new Error("too many elements in array");
    }

    for (let i = 0; i < objLen; i++) {
      const item = obj[i];
      if (item == null) {
        elemData.writeInt32(-1);
      } else {
        subCodec.encode(elemData, item);
      }
    }
    const elemBuf = elemData.unwrap();

    buf.writeInt32(8 + 8 + 1 + elemBuf.length);
    buf.writeInt32(1); // number of dimensions
    buf.writeInt32(0); // flags

    buf.writeInt32(objLen);
    buf.writeInt32(1);

    buf.writeBuffer(elemBuf);
  }

  decode(buf: ReadBuffer): any {
    const ndims = buf.readInt32();
    if (ndims === 0) {
      return EMPTY_ARRAY;
    }
    if (ndims !== 1) {
      throw new Error("only 1-dimensional arrays are supported");
    }

    buf.discard(4); // ignore flags

    const len = buf.readUInt32();
    if (this.len !== -1 && len !== this.len) {
      throw new Error(
        `invalid array size: received ${len}, expected ${this.len}`
      );
    }

    buf.discard(4); // ignore the lower bound info

    const result = new Array(len);
    const elemBuf = ReadBuffer.alloc();
    const subCodec = this.subCodec;

    for (let i = 0; i < len; i++) {
      const elemLen = buf.readInt32();
      if (elemLen === -1) {
        result[i] = null;
      } else {
        buf.consumeInto(elemBuf, elemLen);
        result[i] = subCodec.decode(elemBuf);
      }
    }

    return Object.freeze(result) as any;
  }
}

function isTypedArray(obj: any): obj is any[] {
  return !!(obj.buffer instanceof ArrayBuffer && obj.BYTES_PER_ELEMENT);
}