using System.IO;
using System.Threading;
using SharpCompress.Common;

namespace SharpCompress.Readers
{
    public static class IReaderExtensions
    {
        public static void WriteEntryTo(this IReader reader, string filePath)
        {
            using (Stream stream = File.Open(filePath, FileMode.Create, FileAccess.Write))
            {
                reader.WriteEntryTo(stream);
            }
        }

        public static void WriteEntryTo(this IReader reader, FileInfo filePath)
        {
            using (Stream stream = filePath.Open(FileMode.Create))
            {
                reader.WriteEntryTo(stream);
            }
        }

        /// <summary>
        /// Extract all remaining unread entries to specific directory, retaining filename
        /// </summary>
        public static void WriteAllToDirectory(this IReader reader, string destinationDirectory, ExtractionOptions options,
            CancellationToken cancellationToken)
        {
            while (reader.MoveToNextEntry())
            {
                reader.WriteEntryToDirectory(destinationDirectory, options, cancellationToken);
            }
        }

        /// <summary>
        /// Extract to specific directory, retaining filename
        /// </summary>
        public static void WriteEntryToDirectory(this IReader reader, string destinationDirectory, ExtractionOptions options,
            CancellationToken cancellationToken)
        {
            ExtractionMethods.WriteEntryToDirectory(reader.Entry, destinationDirectory, options, cancellationToken,
                                              reader.WriteEntryToFile);
        }

        /// <summary>
        /// Extract to specific file
        /// </summary>
        public static void WriteEntryToFile(this IReader reader, string destinationFileName, ExtractionOptions options,
            CancellationToken cancellationToken)
        {
            ExtractionMethods.WriteEntryToFile(reader.Entry, destinationFileName, options,
                                               async (x, fm) =>
                                               {
                                                   using (FileStream fs = File.Open(destinationFileName, fm))
                                                   {
                                                       await reader.WriteEntryToAsync(fs,cancellationToken).ConfigureAwait(false);
                                                   }
                                               });
        }
    }
}