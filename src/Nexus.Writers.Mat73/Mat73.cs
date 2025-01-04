using Microsoft.Extensions.Logging;
using Nexus.DataModel;
using Nexus.Extensibility;
using PureHDF;
using PureHDF.Filters;
using PureHDF.Selections;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;

namespace Nexus.Writers;

internal record TextEntry(H5Group Parent, string Key, string Value);

[DataWriterDescription(DESCRIPTION)]
[ExtensionDescription(
    "Store data in Matlab's hierachical data format (v7.3).",
    "https://github.com/Apollo3zehn/nexus-writers-mat73",
    "https://github.com/Apollo3zehn/nexus-writers-mat73")]
public class Mat73 : IDataWriter
{
    private const string DESCRIPTION = """
    {
        "label": "Matlab v7.3 (*.mat)"
    }
    """;

    private const ulong USERBLOCK_SIZE = 512;

    private static readonly JsonSerializerOptions _serializerOptions = new JsonSerializerOptions()
    {
        WriteIndented = true
    };

    private static readonly H5WriteOptions _writeOptions = new H5WriteOptions(
        UserBlockSize: USERBLOCK_SIZE,
        Filters:
        [
            ShuffleFilter.Id,
            DeflateFilter.Id
        ]
    );

    private H5NativeWriter _writer = default!;

    private Stream _fileStream = default!;

    private TimeSpan _lastSamplePeriod;

    private DataWriterContext Context { get; set; } = default!;

    public Task SetContextAsync(
       DataWriterContext context,
       ILogger logger,
       CancellationToken cancellationToken)
    {
        Context = context;
        return Task.CompletedTask;
    }

    public Task OpenAsync(
        DateTime fileBegin,
        TimeSpan filePeriod,
        TimeSpan samplePeriod,
        CatalogItem[] catalogItems,
        CancellationToken cancellationToken)
    {
        return Task.Run(() =>
        {
            _lastSamplePeriod = samplePeriod;

            var totalLength = (ulong)(filePeriod.Ticks / samplePeriod.Ticks);
            var root = Context.ResourceLocator.ToPath();
            var filePath = Path.Combine(root, $"{fileBegin:yyyy-MM-ddTHH-mm-ss}Z_{samplePeriod.ToUnitString()}.mat");

            if (File.Exists(filePath))
                throw new Exception($"The file {filePath} already exists. Extending an already existing file with additional resources is not supported.");

            var h5File = new H5File();

            // file
            var filePropertiesStruct = GetOrCreateMatStruct(h5File, "properties");
            h5File["properties"] = filePropertiesStruct;

            var textEntries = new List<TextEntry>()
            {
                new(filePropertiesStruct, "date_time", fileBegin.ToString("yyyy-MM-ddTHH-mm-ssZ")),
                new(filePropertiesStruct, "sample_period", samplePeriod.ToUnitString())
            };

            foreach (var catalogItemGroup in catalogItems.GroupBy(catalogItem => catalogItem.Catalog.Id))
            {
                cancellationToken.ThrowIfCancellationRequested();

                // file -> catalog
                var catalogId = catalogItemGroup.Key;
                var physicalId = catalogId.TrimStart('/').Replace('/', '_');
                var catalog = catalogItemGroup.First().Catalog;

                var catalogStruct = GetOrCreateMatStruct(h5File, physicalId);

                if (catalog.Properties is not null)
                {
                    var key = "properties";
                    var value = JsonSerializer.Serialize(catalog.Properties, _serializerOptions);
                    textEntries.Add(new TextEntry(catalogStruct, key, value));
                }

                // file -> catalog -> resources
                foreach (var catalogItem in catalogItemGroup)
                {
                    (var chunkLength, var chunkCount) = Utils.CalculateChunkParameters(totalLength);
                    PrepareResource(catalogStruct, catalogItem, chunkLength, chunkCount);
                }
            }

            PrepareAllTextEntries(h5File, textEntries);

            _fileStream = File.Open(
                filePath,
                FileMode.Create, 
                FileAccess.ReadWrite, 
                FileShare.Read
            );

            WritePreamble(_fileStream);
            
            _writer = h5File.BeginWrite(_fileStream, _writeOptions);
        }, cancellationToken);
    }

    public Task WriteAsync(
        TimeSpan fileOffset,
        WriteRequest[] requests,
        IProgress<double> progress,
        CancellationToken cancellationToken)
    {
        return Task.Run(() =>
        {
            var offset = (ulong)(fileOffset.Ticks / _lastSamplePeriod.Ticks);

            var requestGroups = requests
                .GroupBy(request => request.CatalogItem.Catalog.Id)
                .ToList();

            var processed = 0;

            foreach (var requestGroup in requestGroups)
            {
                var catalogId = requestGroup.Key;
                var physicalId = catalogId.TrimStart('/').Replace('/', '_');
                var writeRequests = requestGroup.ToArray();

                for (int i = 0; i < writeRequests.Length; i++)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    WriteData(_writer, physicalId, offset, writeRequests[i]);
                }

                processed++;
                progress.Report((double)processed / requests.Length);
            }
        }, cancellationToken);
    }

    public async Task CloseAsync(CancellationToken cancellationToken)
    {
        _writer?.Dispose();

        if (_fileStream is not null)
            await _fileStream.DisposeAsync();
    }

    private static void WriteData(H5NativeWriter writer, string physicalCatalogId, ulong fileOffset, WriteRequest writeRequest)
    {
        var length = (ulong)writeRequest.Data.Length;
        var catalogGroup = (H5Group)writer.File[physicalCatalogId];
        var resourceGroup = (H5Group)catalogGroup[writeRequest.CatalogItem.Resource.Id];
        var datasetName = $"dataset_{writeRequest.CatalogItem.Representation.Id}{GetRepresentationParameterString(writeRequest.CatalogItem.Parameters)}";
        var dataset = (H5Dataset<Memory<double>>)resourceGroup[datasetName];
        var selection = new HyperslabSelection(fileOffset, length);

        writer.Write(
            dataset: dataset,
            data: MemoryMarshal.AsMemory(writeRequest.Data) /* PureHDF does not yet support ReadOnlyMemory (v2.1.1) */,
            fileSelection: selection);
    }

    private static void PrepareResource(H5Group catalogStruct, CatalogItem catalogItem, uint chunkLength, ulong chunkCount)
    {
        if (chunkLength <= 0)
            throw new Exception("The sample rate is too low.");

        var resourceStruct = GetOrCreateMatStruct(catalogStruct, catalogItem.Resource.Id);

        CreateMatDataset(
            resourceStruct, 
            $"dataset_{catalogItem.Representation.Id}{GetRepresentationParameterString(catalogItem.Parameters)}", 
            chunkLength, 
            chunkCount
        );
    }

    // low level

    private static H5Dataset CreateMatDataset(H5Group parent, string id, uint chunkLength, ulong chunkCount)
    {
        // var fillValue = double.NaN;

        var dataset = new H5Dataset<Memory<double>>(
            fileDims: [chunkLength * chunkCount],
            chunks: [chunkLength]
        )
        {
            Attributes = 
            {
                ["MATLAB_class"] = GetMatTypeFromType(typeof(double))
            }
        };

        parent[id] = dataset;

        return dataset;
    }

    private static H5Group GetOrCreateMatStruct(H5Group parent, string id)
    {
        if (parent.TryGetValue(id, out var value) && value is H5Group group)
        {
            return group;
        }

        else
        {
            var newGroup = new H5Group {
                Attributes = 
                {
                    ["MATLAB_class"] = "struct"
                }
            };

            parent[id] = newGroup;

            return newGroup;
        }
    }

    private static string GetMatTypeFromType(Type type)
    {
        if (type == typeof(double))
            return "double";

        else if (type == typeof(char))
            return "char";

        else if (type == typeof(string))
            return "cell";

        else
            throw new NotImplementedException();
    }

    // low level -> preamble

    private static void WritePreamble(Stream stream)
    {
        var streamData1 = Encoding.ASCII.GetBytes($"MATLAB 7.3 MAT-file, Platform: PCWIN64, Created on: {DateTime.Now.ToString("ddd MMM dd HH:mm:ss yyyy", CultureInfo.InvariantCulture)} HDF5 schema 1.00 .                     ");
        var streamData2 = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x49, 0x4D };
        var streamData3 = new byte[512 - streamData1.Length - streamData2.Length];

        stream.Write(streamData1, 0, streamData1.Length);
        stream.Write(streamData2, 0, streamData2.Length);
        stream.Write(streamData3, 0, streamData3.Length);
    }

    // low level -> string

    private static char GetRefsName(byte index)
    {
        if (index < 26)
            return (char)(index + 0x61);

        else if (index < 54)
            return (char)(index + 0x41);

        else if (index < 63)
            return (char)(index + 0x31);

        else if (index < 64)
            return (char)(index + 0x30);

        else
            throw new ArgumentException("argument 'index' must be < 64");
    }

    private void PrepareAllTextEntries(H5File h5File, IEnumerable<TextEntry> textEntries)
    {
        var refsGroup = new H5Group();
        h5File["#refs#"] = refsGroup;

        var index = (byte)0;

        foreach (var textEntry in textEntries)
        {
            PrepareRefsCellAndTextEntryCellString(
                textEntry, 
                GetRefsName(index).ToString(), 
                refsGroup
            );

            index++;
        }
    }

    private void PrepareRefsCellAndTextEntryCellString(TextEntry textEntry, string refsEntryId, H5Group refsGroup)
    {
        var dataAsBytes = Encoding.Unicode.GetBytes(textEntry.Value);

        #warning PureHDF should be able to work with raw byte arrays
        var data = MemoryMarshal.Cast<byte, ushort>(dataAsBytes).ToArray();

        var dataset = new H5Dataset<ushort[]>(data, fileDims: [(ulong)data.Length, 1])
        {
            Attributes =
            {
                ["MATLAB_class"] = GetMatTypeFromType(typeof(char)),
                ["MATLAB_int_decode"] = new int[] { 2 }
            }
        };

        refsGroup[refsEntryId] = dataset;

        textEntry.Parent[textEntry.Key] = new H5Dataset(
            data: new H5ObjectReference[]
            {
                new(dataset)
            },
            fileDims: [1, 1]
        )
        {
            Attributes = 
            {
                ["MATLAB_class"] = GetMatTypeFromType(typeof(string))
            }
        };
    }

    //private void PrepareRefsTextEntryChar(TextEntry textEntry)
    //{
    //    long datasetId = -1;
    //    long groupId = -1;

    //    bool isNew;

    //    byte index;

    //    try
    //    {
    //        index = 0;

    //        (groupId, isNew) = IOHelper.OpenOrCreateGroup(_fileId, "#refs#");

    //        if (isNew)
    //        {
    //            (datasetId, isNew) = IOHelper.OpenOrCreateDataset(groupId, GetRefsName(index).ToString(), H5T.NATIVE_UINT64, () =>
    //            {
    //                long dataspaceId = -1;

    //                try
    //                {
    //                    dataspaceId = H5S.create_simple(2, new ulong[] { 6, 1 }, null);
    //                    datasetId = H5D.create(groupId, GetRefsName(index).ToString(), H5T.NATIVE_UINT64, dataspaceId);
    //                }
    //                catch (Exception)
    //                {
    //                    if (H5I.is_valid(datasetId) > 0) { H5D.close(datasetId); }
    //                    
    //                    throw;
    //                }
    //                finally
    //                {
    //                    if (H5I.is_valid(dataspaceId) > 0) { H5S.close(dataspaceId); }
    //                }
    //            });

    //            if (isNew)
    //            {
    //                IOHelper.Write(datasetId, new UInt64[] {  }, DataContainerType.Dataset);
    //            }
    //        }

    //private void PrepareTextEntryChar(TextEntry textEntry, byte index)
    //{
    //    long datasetId = -1;
    //    bool isNew;

    //    try
    //    {
    //        (datasetId, isNew) = IOHelper.OpenOrCreateDataset(_fileId, $"{ textEntry.Path }/{ textEntry.Name }", H5T.NATIVE_UINT32, () =>
    //        {
    //            long dataspaceId = -1;

    //            try
    //            {
    //                dataspaceId = H5S.create_simple(2, new UInt64[] { 1, 6 }, null);
    //                datasetId = H5D.create(_fileId, $"{ textEntry.Path }/{ textEntry.Name }", H5T.NATIVE_UINT32, dataspaceId);
    //            }
    //            catch (Exception)
    //            {
    //                if (H5I.is_valid(datasetId) > 0) { H5D.close(datasetId); }
    //                
    //                throw;
    //            }
    //            finally
    //            {
    //                if (H5I.is_valid(dataspaceId) > 0) { H5S.close(dataspaceId); }
    //            }

    //            return datasetId;
    //        });

    //        if (isNew)
    //        {
    //            IOHelper.Write(datasetId, new UInt32[] { 0xDD000000, 0x02, 0x01, 0x01, index, 0x01 }, DataContainerType.Dataset);
    //        }

    //        PrepareStringAttribute(datasetId, "MATLAB_class", GetMatTypeFromType(typeof(string)));
    //        PrepareInt32Attribute(datasetId, "MATLAB_object_decode", 3);
    //    }
    //    finally
    //    {
    //        if (H5I.is_valid(datasetId) > 0) { H5D.close(datasetId); }
    //    }
    //}

    private static string? GetRepresentationParameterString(IReadOnlyDictionary<string, string>? parameters)
    {
        if (parameters is null)
            return default;



        var serializedParameters = parameters
            .Select(parameter =>
            {
                if (!TryEnforceNamingConvention(parameter.Value, out var newValue))
                    throw new Exception("Unable to ensure valid variable name.");

                return $"{parameter.Key}_{newValue}";
            });

        var parametersString = $"_{string.Join('_', serializedParameters)}";

        return parametersString;
    }

    private static bool TryEnforceNamingConvention(string parameterName, [NotNullWhen(returnValue: true)] out string newParameterName)
    {
        newParameterName = parameterName;
        newParameterName = Resource.InvalidIdCharsExpression.Replace(newParameterName, "");
        newParameterName = Resource.InvalidIdStartCharsExpression.Replace(newParameterName, "");

        return Resource.ValidIdExpression.IsMatch(newParameterName);
    }
}