using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using SharpCompress.Common;

namespace SharpCompress.Writers
{
    public abstract class AbstractWriter : IWriter
    {
        private bool _isDisposed;

        protected AbstractWriter(ArchiveType type, WriterOptions writerOptions)
        {
            WriterType = type;
            WriterOptions = writerOptions;
        }

        protected void InitalizeStream(Stream stream)
        {
            OutputStream = stream;
        }

        protected Stream OutputStream { get; private set; }

        public ArchiveType WriterType { get; }

        protected WriterOptions WriterOptions { get; }

        public abstract void Write(string filename, Stream source, DateTime? modificationTime);

        public abstract Task WriteAsync(string filename, Stream source, DateTime? modificationTime, CancellationToken cancellationToken);

        protected virtual void Dispose(bool isDisposing)
        {
            if (isDisposing)
            {
                OutputStream.Dispose();
            }
        }

        public void Dispose()
        {
            if (!_isDisposed)
            {
                GC.SuppressFinalize(this);
                Dispose(true);
                _isDisposed = true;
            }
        }

        ~AbstractWriter()
        {
            if (!_isDisposed)
            {
                Dispose(false);
                _isDisposed = true;
            }
        }
    }
}