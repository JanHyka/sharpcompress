using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace SharpCompress.Test.Mocks
{
    // This is a simplified version of CryptoStream that always flushes the inner stream on Dispose to trigger an error in EntryStream
    // CryptoStream doesn't always trigger the Flush, so this class is used instead
    // See https://referencesource.microsoft.com/#mscorlib/system/security/cryptography/cryptostream.cs,141

    public class FlushOnDisposeStream : Stream
    {
        private Stream inner;

        public FlushOnDisposeStream(Stream innerStream) {
            this.inner = innerStream;
        }

        public override bool CanRead => this.inner.CanRead;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Length => this.inner.Length;

        public override long Position { get => this.inner.Position; set => this.inner.Position = value; }

        public override void Flush() {
            throw new NotImplementedException();
        }

        public override int Read(byte[] buffer, int offset, int count) {
            throw new NotImplementedException();
            //return this.inner.Read(buffer, offset, count);
        }

        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            return await this.inner.ReadAsync(buffer, offset, count, cancellationToken).ConfigureAwait(false);
        }

        public override long Seek(long offset, SeekOrigin origin) {
            throw new NotImplementedException();
        }

        public override void SetLength(long value) {
            throw new NotImplementedException();
        }

        public override void Write(byte[] buffer, int offset, int count) {
            throw new NotImplementedException();
        }

        protected override void Dispose(bool disposing) {
            if(disposing) {
                this.inner.Flush();
                this.inner.Close();
            }

            base.Dispose(disposing);
        }
    }
}
