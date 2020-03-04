using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using SharpCompress.Common;
using SharpCompress.Common.Tar.Headers;
using SharpCompress.Compressors;
using SharpCompress.Compressors.BZip2;
using SharpCompress.Compressors.Deflate;
using SharpCompress.Compressors.Deflate64;
using SharpCompress.Compressors.LZMA;
using SharpCompress.IO;

namespace SharpCompress.Writers.Tar
{
    public class TarWriter : AbstractWriter
    {
        private readonly bool finalizeArchiveOnClose;

        public TarWriter(Stream destination, TarWriterOptions options)
            : base(ArchiveType.Tar, options)
        {
            finalizeArchiveOnClose = options.FinalizeArchiveOnClose;

            if (!destination.CanWrite)
            {
                throw new ArgumentException("Tars require writable streams.");
            }
            if (WriterOptions.LeaveStreamOpen)
            {
                destination = new NonDisposingStream(destination);
            }
            switch (options.CompressionType)
            {
                case CompressionType.None:
                    break;
                case CompressionType.BZip2:
                {
                    destination = new BZip2Stream(destination, CompressionMode.Compress, false);
                }
                    break;
                case CompressionType.GZip:
                {
                    destination = new GZipStream(destination, CompressionMode.Compress);
                }
                    break;
                case CompressionType.LZip:
                {
                    destination = new LZipStream(destination, CompressionMode.Compress);
                }
                    break;
                default:
                {
                    throw new InvalidFormatException("Tar does not support compression: " + options.CompressionType);
                }
            }
            InitalizeStream(destination);
        }

        public override void Write(string filename, Stream source, DateTime? modificationTime)
        {
            Write(filename, source, modificationTime, null);
        }

        public override async Task WriteAsync(string filename, Stream source, DateTime? modificationTime, CancellationToken cancellationToken)
        {
            await WriteAsync(filename, source, modificationTime, null, cancellationToken).ConfigureAwait(false);
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

        public void Write(string filename, Stream source, DateTime? modificationTime, long? size)
        {
            WriteAsync(filename, source, modificationTime, size, CancellationToken.None).GetAwaiter().GetResult();
        }

        public async Task WriteAsync(string filename, Stream source, DateTime? modificationTime, long? size, CancellationToken cancellationToken)
        {
            if (!source.CanSeek && size == null)
            {
                throw new ArgumentException("Seekable stream is required if no size is given.");
            }

            long realSize = size ?? source.Length;

            TarHeader header = new TarHeader(WriterOptions.ArchiveEncoding)
            {
                LastModifiedTime = modificationTime ?? TarHeader.EPOCH,
                Name = NormalizeFilename(filename),
                Size = realSize
            };
            await header.WriteAsync(OutputStream, cancellationToken).ConfigureAwait(false);
            size = await source.TransferTo(OutputStream, cancellationToken).ConfigureAwait(false);
            await PadTo512(size.Value, false, cancellationToken).ConfigureAwait(false);
        }

        private async Task PadTo512(long size, bool forceZeros,CancellationToken cancellationToken)
        {
            int zeros = (int)size % 512;
            if (zeros == 0 && !forceZeros)
            {
                return;
            }
            zeros = 512 - zeros;
            await OutputStream.WriteAsync(new byte[zeros], 0, zeros, cancellationToken).ConfigureAwait(false);
        }

        protected override void Dispose(bool isDisposing)
        {
            if (isDisposing)
            {
                if (finalizeArchiveOnClose) {
                    PadTo512(0, true, CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();
                    PadTo512(0, true, CancellationToken.None).ConfigureAwait(false).GetAwaiter().GetResult();
                }
                switch (OutputStream)
                {
                    case BZip2Stream b:
                    {
                        b.Finish();
                        break;
                    }
                    case LZipStream l:
                    {
                        l.Finish();
                        break;
                    }
                }
            }
            base.Dispose(isDisposing);
        }

    }
}