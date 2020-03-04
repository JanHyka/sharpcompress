using System.Linq;
using System.Threading;
using SharpCompress.Common;

namespace SharpCompress.Archives
{
    public static class IArchiveExtensions
    {
        /// <summary>
        /// Extract to specific directory, retaining filename
        /// </summary>
        public static void WriteToDirectory(this IArchive archive, string destinationDirectory,
                                            ExtractionOptions options = null)
        {
            archive.WriteToDirectory(destinationDirectory, CancellationToken.None, options);
        }

        public static void WriteToDirectory(this IArchive archive, string destinationDirectory, CancellationToken cancellationToken,
            ExtractionOptions options = null)
        {
            foreach (IArchiveEntry entry in archive.Entries.Where(x => !x.IsDirectory))
            {
                entry.WriteToDirectory(destinationDirectory, options, cancellationToken);
            }
        }
    }
}