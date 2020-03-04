using System.IO;
using System.Threading;
using System.Threading.Tasks;
using SharpCompress.Common.Zip.Headers;
using SharpCompress.Compressors.Deflate;
using SharpCompress.IO;

namespace SharpCompress.Common.Zip
{
    internal class StreamingZipFilePart : ZipFilePart
    {
        private Stream _decompressionStream;

        internal StreamingZipFilePart(ZipFileEntry header, Stream stream)
            : base(header, stream)
        {
        }

        protected override Stream CreateBaseStream()
        {
            return Header.PackedStream;
        }

        internal override Stream GetCompressedStream()
        {
            if (!Header.HasData)
            {
                return Stream.Null;
            }
            _decompressionStream = CreateDecompressionStream(GetCryptoStream(CreateBaseStream()), Header.CompressionMethod);
            if (LeaveStreamOpen)
            {
                return new NonDisposingStream(_decompressionStream);
            }
            return _decompressionStream;
        }

        internal async Task<BinaryReader> FixStreamedFileLocation(RewindableStream rewindableStream, CancellationToken cancellationToken)
        {
            if (Header.IsDirectory)
            {
                return new BinaryReader(rewindableStream);
            }
            if (Header.HasData && !Skipped)
            {
                if (_decompressionStream == null)
                {
                    _decompressionStream = GetCompressedStream();
                }
                _decompressionStream.Skip();

                DeflateStream deflateStream = _decompressionStream as DeflateStream;
                if (deflateStream != null)
                {
                    await rewindableStream.Rewind(deflateStream.InputBuffer, cancellationToken).ConfigureAwait(false);
                }
                Skipped = true;
            }
            var reader = new BinaryReader(rewindableStream);
            _decompressionStream = null;
            return reader;
        }
    }
}