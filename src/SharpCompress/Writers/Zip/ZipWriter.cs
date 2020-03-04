﻿using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using SharpCompress.Common;
using SharpCompress.Common.Zip;
using SharpCompress.Common.Zip.Headers;
using SharpCompress.Compressors;
using SharpCompress.Compressors.BZip2;
using SharpCompress.Compressors.Deflate;
using SharpCompress.Compressors.LZMA;
using SharpCompress.Compressors.PPMd;
using SharpCompress.IO;

namespace SharpCompress.Writers.Zip
{
    public class ZipWriter : AbstractWriter
    {
        private readonly CompressionType compressionType;
        private readonly CompressionLevel compressionLevel;
        private readonly List<ZipCentralDirectoryEntry> entries = new List<ZipCentralDirectoryEntry>();
        private readonly string zipComment;
        private long streamPosition;
        private PpmdProperties ppmdProps;
        private readonly bool isZip64;

        public ZipWriter(Stream destination, ZipWriterOptions zipWriterOptions)
            : base(ArchiveType.Zip, zipWriterOptions)
        {
            zipComment = zipWriterOptions.ArchiveComment ?? string.Empty;
            isZip64 = zipWriterOptions.UseZip64;
            if (destination.CanSeek)
            {
                streamPosition = destination.Position;
            }

            compressionType = zipWriterOptions.CompressionType;
            compressionLevel = zipWriterOptions.DeflateCompressionLevel;

            if (WriterOptions.LeaveStreamOpen)
            {
                destination = new NonDisposingStream(destination);
            }
            InitalizeStream(destination);
        }

        private PpmdProperties PpmdProperties
        {
            get
            {
                if (ppmdProps == null)
                {
                    ppmdProps = new PpmdProperties();
                }
                return ppmdProps;
            }
        }

        protected override void Dispose(bool isDisposing)
        {
            if (isDisposing)
            {
                ulong size = 0;
                foreach (ZipCentralDirectoryEntry entry in entries)
                {
                    size += entry.Write(OutputStream);
                }
                WriteEndRecord(size, CancellationToken.None).GetAwaiter().GetResult();
            }
            base.Dispose(isDisposing);
        }

        private static ZipCompressionMethod ToZipCompressionMethod(CompressionType compressionType)
        {
            switch (compressionType)
            {
                case CompressionType.None:
                    {
                        return ZipCompressionMethod.None;
                    }
                case CompressionType.Deflate:
                    {
                        return ZipCompressionMethod.Deflate;
                    }
                case CompressionType.BZip2:
                    {
                        return ZipCompressionMethod.BZip2;
                    }
                case CompressionType.LZMA:
                    {
                        return ZipCompressionMethod.LZMA;
                    }
                case CompressionType.PPMd:
                    {
                        return ZipCompressionMethod.PPMd;
                    }
                default:
                    throw new InvalidFormatException("Invalid compression method: " + compressionType);
            }
        }

        public override void Write(string entryPath, Stream source, DateTime? modificationTime)
        {
            Write(entryPath, source, new ZipWriterEntryOptions()
            {
                ModificationDateTime = modificationTime
            });
        }

        public void Write(string entryPath, Stream source, ZipWriterEntryOptions zipWriterEntryOptions)
        {
            WriteAsync(entryPath, source, zipWriterEntryOptions, CancellationToken.None).GetAwaiter().GetResult();
        }

        public async Task WriteAsync(string entryPath, Stream source, ZipWriterEntryOptions zipWriterEntryOptions, CancellationToken cancellationToken)
        {
            using (Stream output = await WriteToStreamAsync(entryPath, zipWriterEntryOptions,cancellationToken).ConfigureAwait(false))
            {
                await source.TransferTo(output, cancellationToken).ConfigureAwait(false);
            }
        }

        public Stream WriteToStream(string entryPath, ZipWriterEntryOptions options)
        {
            return WriteToStreamAsync(entryPath, options, CancellationToken.None).GetAwaiter().GetResult();
        }

        public async Task<Stream> WriteToStreamAsync(string entryPath, ZipWriterEntryOptions options, CancellationToken cancellationToken)
        {
            var compression = ToZipCompressionMethod(options.CompressionType ?? compressionType);

            entryPath = NormalizeFilename(entryPath);
            options.ModificationDateTime ??= DateTime.Now;
            options.EntryComment ??= string.Empty;
            var entry = new ZipCentralDirectoryEntry(compression, entryPath, (ulong)streamPosition, WriterOptions.ArchiveEncoding)
            {
                Comment = options.EntryComment,
                ModificationTime = options.ModificationDateTime
            };

            // Use the archive default setting for zip64 and allow overrides
            var useZip64 = isZip64;
            if (options.EnableZip64.HasValue)
                useZip64 = options.EnableZip64.Value;

            var headersize = (uint) await WriteHeader(entryPath, options, entry, useZip64, cancellationToken).ConfigureAwait(false);
            streamPosition += headersize;
            return new ZipWritingStream(this, OutputStream, entry, compression,
                options.DeflateCompressionLevel ?? compressionLevel);
        }

        private string NormalizeFilename(string filename)
        {
            filename = filename.Replace('\\', '/');

            int pos = filename.IndexOf(':');
            if (pos >= 0)
            {
                filename = filename.Remove(0, pos + 1);
            }

            return filename.Trim('/');
        }

        private async Task<uint> WriteHeader(string filename, ZipWriterEntryOptions zipWriterEntryOptions, ZipCentralDirectoryEntry entry, bool useZip64, CancellationToken cancellationToken)
        {
            // We err on the side of caution until the zip specification clarifies how to support this
            if (!OutputStream.CanSeek && useZip64)
                throw new NotSupportedException("Zip64 extensions are not supported on non-seekable streams");

            var explicitZipCompressionInfo = ToZipCompressionMethod(zipWriterEntryOptions.CompressionType ?? compressionType);
            byte[] encodedFilename = WriterOptions.ArchiveEncoding.Encode(filename);

            // TODO: Use stackalloc when we exclusively support netstandard2.1 or higher
            byte[] intBuf = new byte[4];
            BinaryPrimitives.WriteUInt32LittleEndian(intBuf, ZipHeaderFactory.ENTRY_HEADER_BYTES);
            await OutputStream.WriteAsync(intBuf, 0, 4, cancellationToken).ConfigureAwait(false);
            if (explicitZipCompressionInfo == ZipCompressionMethod.Deflate)
            {
                if (OutputStream.CanSeek && useZip64)
                    await OutputStream.WriteAsync(new byte[] { 45, 0 }, 0, 2, cancellationToken).ConfigureAwait(false); //smallest allowed version for zip64
                else
                    await OutputStream.WriteAsync(new byte[] { 20, 0 }, 0, 2, cancellationToken).ConfigureAwait(false); //older version which is more compatible
            }
            else
            {
                await OutputStream.WriteAsync(new byte[] { 63, 0 }, 0, 2, cancellationToken).ConfigureAwait(false); //version says we used PPMd or LZMA
            }
            HeaderFlags flags = Equals(WriterOptions.ArchiveEncoding.GetEncoding(), Encoding.UTF8) ? HeaderFlags.Efs : 0;
            if (!OutputStream.CanSeek)
            {
                flags |= HeaderFlags.UsePostDataDescriptor;

                if (explicitZipCompressionInfo == ZipCompressionMethod.LZMA)
                {
                    flags |= HeaderFlags.Bit1; // eos marker
                }
            }

            BinaryPrimitives.WriteUInt16LittleEndian(intBuf, (ushort)flags);
            await OutputStream.WriteAsync(intBuf, 0, 2, cancellationToken).ConfigureAwait(false);
            BinaryPrimitives.WriteUInt16LittleEndian(intBuf, (ushort)explicitZipCompressionInfo);
            await OutputStream.WriteAsync(intBuf, 0, 2, cancellationToken).ConfigureAwait(false); // zipping method
            BinaryPrimitives.WriteUInt32LittleEndian(intBuf, zipWriterEntryOptions.ModificationDateTime.DateTimeToDosTime());
            await OutputStream.WriteAsync(intBuf, 0, 4, cancellationToken).ConfigureAwait(false);

            // zipping date and time
            await OutputStream.WriteAsync(new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, 0, 12, cancellationToken).ConfigureAwait(false);

            // unused CRC, un/compressed size, updated later
            BinaryPrimitives.WriteUInt16LittleEndian(intBuf, (ushort)encodedFilename.Length);
            await OutputStream.WriteAsync(intBuf, 0, 2, cancellationToken).ConfigureAwait(false); // filename length

            var extralength = 0;
            if (OutputStream.CanSeek && useZip64)
                extralength = 2 + 2 + 8 + 8;

            BinaryPrimitives.WriteUInt16LittleEndian(intBuf, (ushort)extralength);
            await OutputStream.WriteAsync(intBuf, 0, 2, cancellationToken).ConfigureAwait(false); // extra length
            OutputStream.Write(encodedFilename, 0, encodedFilename.Length);

            if (extralength != 0)
            {
                await OutputStream.WriteAsync(new byte[extralength], 0, extralength).ConfigureAwait(false); // reserve space for zip64 data
                entry.Zip64HeaderOffset = (ushort)(6 + 2 + 2 + 4 + 12 + 2 + 2 + encodedFilename.Length);
            }

            return (uint)(6 + 2 + 2 + 4 + 12 + 2 + 2 + encodedFilename.Length + extralength);
        }

        private void WriteFooter(uint crc, uint compressed, uint uncompressed)
        {
            byte[] intBuf = new byte[4];
            BinaryPrimitives.WriteUInt32LittleEndian(intBuf, crc);
            OutputStream.Write(intBuf, 0, 4);
            BinaryPrimitives.WriteUInt32LittleEndian(intBuf, compressed);
            OutputStream.Write(intBuf, 0, 4);
            BinaryPrimitives.WriteUInt32LittleEndian(intBuf, uncompressed);
            OutputStream.Write(intBuf, 0, 4);
        }

        private async Task WriteEndRecord(ulong size, CancellationToken cancellationToken)
        {

            var zip64 = isZip64 || entries.Count > ushort.MaxValue || streamPosition >= uint.MaxValue || size >= uint.MaxValue;

            var sizevalue = size >= uint.MaxValue ? uint.MaxValue : (uint)size;
            var streampositionvalue = streamPosition >= uint.MaxValue ? uint.MaxValue : (uint)streamPosition;

            byte[] intBuf = new byte[8];
            if (zip64)
            {
                var recordlen = 2 + 2 + 4 + 4 + 8 + 8 + 8 + 8;

                // Write zip64 end of central directory record
                await OutputStream.WriteAsync(new byte[] { 80, 75, 6, 6 }, 0, 4, cancellationToken).ConfigureAwait(false);

                BinaryPrimitives.WriteUInt64LittleEndian(intBuf, (ulong)recordlen);
                await OutputStream.WriteAsync(intBuf, 0, 8, cancellationToken).ConfigureAwait(false); // Size of zip64 end of central directory record
                BinaryPrimitives.WriteUInt16LittleEndian(intBuf, 0);
                await OutputStream.WriteAsync(intBuf, 0, 2, cancellationToken).ConfigureAwait(false); // Made by
                BinaryPrimitives.WriteUInt16LittleEndian(intBuf, 45);
                await OutputStream.WriteAsync(intBuf, 0, 2, cancellationToken).ConfigureAwait(false); // Version needed

                BinaryPrimitives.WriteUInt32LittleEndian(intBuf, 0);
                await OutputStream.WriteAsync(intBuf, 0, 4, cancellationToken).ConfigureAwait(false); // Disk number
                await OutputStream.WriteAsync(intBuf, 0, 4, cancellationToken).ConfigureAwait(false); // Central dir disk

                // TODO: entries.Count is int, so max 2^31 files
                BinaryPrimitives.WriteUInt64LittleEndian(intBuf, (ulong)entries.Count);
                await OutputStream.WriteAsync(intBuf, 0, 8, cancellationToken).ConfigureAwait(false); // Entries in this disk
                await OutputStream.WriteAsync(intBuf, 0, 8, cancellationToken).ConfigureAwait(false); // Total entries
                BinaryPrimitives.WriteUInt64LittleEndian(intBuf, size);
                await OutputStream.WriteAsync(intBuf, 0, 8, cancellationToken).ConfigureAwait(false); // Central Directory size
                BinaryPrimitives.WriteUInt64LittleEndian(intBuf, (ulong)streamPosition);
                await OutputStream.WriteAsync(intBuf, 0, 8, cancellationToken).ConfigureAwait(false); // Disk offset

                // Write zip64 end of central directory locator
                await OutputStream.WriteAsync(new byte[] { 80, 75, 6, 7 }, 0, 4, cancellationToken).ConfigureAwait(false);

                BinaryPrimitives.WriteUInt32LittleEndian(intBuf, 0);
                await OutputStream.WriteAsync(intBuf, 0, 4, cancellationToken).ConfigureAwait(false); // Entry disk
                BinaryPrimitives.WriteUInt64LittleEndian(intBuf, (ulong)streamPosition + size);
                await OutputStream.WriteAsync(intBuf, 0, 8, cancellationToken).ConfigureAwait(false); // Offset to the zip64 central directory
                BinaryPrimitives.WriteUInt32LittleEndian(intBuf, 0);
                await OutputStream.WriteAsync(intBuf, 0, 4, cancellationToken).ConfigureAwait(false); // Number of disks

                streamPosition += recordlen + (4 + 4 + 8 + 4);
                streampositionvalue = streamPosition >= uint.MaxValue ? uint.MaxValue : (uint)streampositionvalue;
            }

            // Write normal end of central directory record
            await OutputStream.WriteAsync(new byte[] { 80, 75, 5, 6, 0, 0, 0, 0 }, 0, 8, cancellationToken).ConfigureAwait(false);
            BinaryPrimitives.WriteUInt16LittleEndian(intBuf, (ushort)entries.Count);
            await OutputStream.WriteAsync(intBuf, 0, 2, cancellationToken).ConfigureAwait(false);
            await OutputStream.WriteAsync(intBuf, 0, 2, cancellationToken).ConfigureAwait(false);
            BinaryPrimitives.WriteUInt32LittleEndian(intBuf, sizevalue);
            await OutputStream.WriteAsync(intBuf, 0, 4, cancellationToken).ConfigureAwait(false);
            BinaryPrimitives.WriteUInt32LittleEndian(intBuf, streampositionvalue);
            await OutputStream.WriteAsync(intBuf, 0, 4, cancellationToken).ConfigureAwait(false);
            byte[] encodedComment = WriterOptions.ArchiveEncoding.Encode(zipComment);
            BinaryPrimitives.WriteUInt16LittleEndian(intBuf, (ushort)encodedComment.Length);
            await OutputStream.WriteAsync(intBuf, 0, 2, cancellationToken).ConfigureAwait(false);
            await OutputStream.WriteAsync(encodedComment, 0, encodedComment.Length, cancellationToken).ConfigureAwait(false);
        }

        public override Task WriteAsync(string filename, Stream source, DateTime? modificationTime, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        #region Nested type: ZipWritingStream

        internal class ZipWritingStream : Stream
        {
            private readonly CRC32 crc = new CRC32();
            private readonly ZipCentralDirectoryEntry entry;
            private readonly Stream originalStream;
            private readonly Stream writeStream;
            private readonly ZipWriter writer;
            private readonly ZipCompressionMethod zipCompressionMethod;
            private readonly CompressionLevel compressionLevel;
            private CountingWritableSubStream counting;
            private ulong decompressed;

            // Flag to prevent throwing exceptions on Dispose
            private bool limitsExceeded;
            private bool isDisposed;

            internal ZipWritingStream(ZipWriter writer, Stream originalStream, ZipCentralDirectoryEntry entry,
                ZipCompressionMethod zipCompressionMethod, CompressionLevel compressionLevel)
            {
                this.writer = writer;
                this.originalStream = originalStream;
                this.writer = writer;
                this.entry = entry;
                this.zipCompressionMethod = zipCompressionMethod;
                this.compressionLevel = compressionLevel;
                writeStream = GetWriteStream(originalStream);
            }

            public override bool CanRead => false;

            public override bool CanSeek => false;

            public override bool CanWrite => true;

            public override long Length => throw new NotSupportedException();

            public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

            private Stream GetWriteStream(Stream writeStream)
            {
                counting = new CountingWritableSubStream(writeStream);
                Stream output = counting;
                switch (zipCompressionMethod)
                {
                    case ZipCompressionMethod.None:
                        {
                            return output;
                        }
                    case ZipCompressionMethod.Deflate:
                        {
                            return new DeflateStream(counting, CompressionMode.Compress, compressionLevel);
                        }
                    case ZipCompressionMethod.BZip2:
                        {
                            return new BZip2Stream(counting, CompressionMode.Compress, false);
                        }
                    case ZipCompressionMethod.LZMA:
                        {
                            counting.WriteByte(9);
                            counting.WriteByte(20);
                            counting.WriteByte(5);
                            counting.WriteByte(0);

                            LzmaStream lzmaStream = new LzmaStream(new LzmaEncoderProperties(!originalStream.CanSeek),
                                                                   false, counting);
                            counting.Write(lzmaStream.Properties, 0, lzmaStream.Properties.Length);
                            return lzmaStream;
                        }
                    case ZipCompressionMethod.PPMd:
                        {
                            counting.Write(writer.PpmdProperties.Properties, 0, 2);
                            return new PpmdStream(writer.PpmdProperties, counting, true);
                        }
                    default:
                        {
                            throw new NotSupportedException("CompressionMethod: " + zipCompressionMethod);
                        }
                }
            }

            protected override void Dispose(bool disposing)
            {
                if (isDisposed)
                {
                    return;
                }

                isDisposed = true;

                base.Dispose(disposing);
                if (disposing)
                {
                    writeStream.Dispose();

                    if (limitsExceeded)
                    {
                        // We have written invalid data into the archive,
                        // so we destroy it now, instead of allowing the user to continue
                        // with a defunct archive
                        originalStream.Dispose();
                        return;
                    }

                    entry.Crc = (uint)crc.Crc32Result;
                    entry.Compressed = counting.Count;
                    entry.Decompressed = decompressed;

                    var zip64 = entry.Compressed >= uint.MaxValue || entry.Decompressed >= uint.MaxValue;
                    var compressedvalue = zip64 ? uint.MaxValue : (uint)counting.Count;
                    var decompressedvalue = zip64 ? uint.MaxValue : (uint)entry.Decompressed;

                    if (originalStream.CanSeek)
                    {
                        originalStream.Position = (long)(entry.HeaderOffset + 6);
                        originalStream.WriteByte(0);

                        if (counting.Count == 0 && entry.Decompressed == 0)
                        {
                            // set compression to STORED for zero byte files (no compression data)
                            originalStream.Position = (long)(entry.HeaderOffset + 8);
                            originalStream.WriteByte(0);
                            originalStream.WriteByte(0);
                        }

                        originalStream.Position = (long)(entry.HeaderOffset + 14);

                        writer.WriteFooter(entry.Crc, compressedvalue, decompressedvalue);

                        // Ideally, we should not throw from Dispose()
                        // We should not get here as the Write call checks the limits
                        if (zip64 && entry.Zip64HeaderOffset == 0)
                            throw new NotSupportedException("Attempted to write a stream that is larger than 4GiB without setting the zip64 option");

                        // If we have pre-allocated space for zip64 data,
                        // fill it out, even if it is not required
                        if (entry.Zip64HeaderOffset != 0)
                        {
                            originalStream.Position = (long)(entry.HeaderOffset + entry.Zip64HeaderOffset);
                            byte[] intBuf = new byte[8];
                            BinaryPrimitives.WriteUInt16LittleEndian(intBuf, 0x0001);
                            originalStream.Write(intBuf, 0, 2);
                            BinaryPrimitives.WriteUInt16LittleEndian(intBuf, 8 + 8);
                            originalStream.Write(intBuf, 0, 2);

                            BinaryPrimitives.WriteUInt64LittleEndian(intBuf, entry.Decompressed);
                            originalStream.Write(intBuf, 0, 8);
                            BinaryPrimitives.WriteUInt64LittleEndian(intBuf, entry.Compressed);
                            originalStream.Write(intBuf, 0, 8);
                        }

                        originalStream.Position = writer.streamPosition + (long)entry.Compressed;
                        writer.streamPosition += (long)entry.Compressed;
                    }
                    else
                    {
                        // We have a streaming archive, so we should add a post-data-descriptor,
                        // but we cannot as it does not hold the zip64 values
                        // Throwing an exception until the zip specification is clarified

                        // Ideally, we should not throw from Dispose()
                        // We should not get here as the Write call checks the limits
                        if (zip64)
                            throw new NotSupportedException("Streams larger than 4GiB are not supported for non-seekable streams");

                        byte[] intBuf = new byte[4];
                        BinaryPrimitives.WriteUInt32LittleEndian(intBuf, ZipHeaderFactory.POST_DATA_DESCRIPTOR);
                        originalStream.Write(intBuf, 0, 4);
                        writer.WriteFooter(entry.Crc,
                                           compressedvalue,
                                           decompressedvalue);
                        writer.streamPosition += (long)entry.Compressed + 16;
                    }
                    writer.entries.Add(entry);
                }
            }

            public override void Flush()
            {
                writeStream.Flush();
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                throw new NotSupportedException();
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                throw new NotSupportedException();
            }

            public override void SetLength(long value)
            {
                throw new NotSupportedException();
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                // We check the limits first, because we can keep the archive consistent
                // if we can prevent the writes from happening
                if (entry.Zip64HeaderOffset == 0)
                {
                    // Pre-check, the counting.Count is not exact, as we do not know the size before having actually compressed it
                    if (limitsExceeded || ((decompressed + (uint)count) > uint.MaxValue) || (counting.Count + (uint)count) > uint.MaxValue)
                        throw new NotSupportedException("Attempted to write a stream that is larger than 4GiB without setting the zip64 option");
                }

                decompressed += (uint)count;
                crc.SlurpBlock(buffer, offset, count);
                writeStream.Write(buffer, offset, count);

                if (entry.Zip64HeaderOffset == 0)
                {
                    // Post-check, this is accurate
                    if ((decompressed > uint.MaxValue) || counting.Count > uint.MaxValue)
                    {
                        // We have written the data, so the archive is now broken
                        // Throwing the exception here, allows us to avoid
                        // throwing an exception in Dispose() which is discouraged
                        // as it can mask other errors
                        limitsExceeded = true;
                        throw new NotSupportedException("Attempted to write a stream that is larger than 4GiB without setting the zip64 option");
                    }
                }
            }
        }

        #endregion Nested type: ZipWritingStream
    }
}
