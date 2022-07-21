using HDF.PInvoke;
using Microsoft.Extensions.Logging;
using Nexus.DataModel;
using Nexus.Extensibility;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Nexus.Writers
{
    [DataWriterDescription(DESCRIPTION)]
    [ExtensionDescription(
        "Store data in Matlab's hierachical data format (v7.3).",
        "https://github.com/Apollo3zehn/nexus-writers-mat73",
        "https://github.com/Apollo3zehn/nexus-writers-mat73")]
    public class Mat73 : IDataWriter
    {
        #region "Fields"

private const string DESCRIPTION = @"
{
  ""label"": ""Matlab v7.3 (*.mat)"",
}
        ";

        private const ulong USERBLOCK_SIZE = 512;

        private long _fileId = -1;
        private TimeSpan _lastSamplePeriod;
        private JsonSerializerOptions _serializerOptions;

        #endregion

        #region Constructors

        public Mat73()
        {
            _serializerOptions = new JsonSerializerOptions()
            {
                WriteIndented = true
            };
        }

        #endregion

        #region Properties

        private DataWriterContext Context { get; set; } = default!;

        #endregion

        #region "Methods"

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
                var filePath = Path.Combine(root, $"{fileBegin.ToString("yyyy-MM-ddTHH-mm-ss")}Z_{samplePeriod.ToUnitString()}.mat");

                if (File.Exists(filePath))
                    throw new Exception($"The file {filePath} already exists. Extending an already existing file with additional resources is not supported.");

                long propertyId = -1;

                try
                {
                    propertyId = H5P.create(H5P.FILE_CREATE);
                    H5P.set_userblock(propertyId, USERBLOCK_SIZE);
                    _fileId = H5F.create(filePath, H5F.ACC_TRUNC, propertyId);

                    if (_fileId < 0)
                        throw new Exception($"{ ErrorMessage.Mat73Writer_CouldNotOpenOrCreateFile } File: { filePath }.");

                    // file
                    var textEntries = new List<TextEntry>()
                    {
                        new TextEntry("/properties", "system_name", "Nexus"),
                        new TextEntry("/properties", "date_time", fileBegin.ToString("yyyy-MM-ddTHH-mm-ss") + "Z"),
                        new TextEntry("/properties", "sample_period", samplePeriod.ToUnitString())
                    };

                    foreach (var catalogItemGroup in catalogItems.GroupBy(catalogItem => catalogItem.Catalog))
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        // file -> catalog
                        var catalog = catalogItemGroup.Key;
                        var physicalId = catalog.Id.TrimStart('/').Replace('/', '_');

                        if (catalog.Properties.HasValue)
                        {
                            var key = "properties";
                            var value = JsonSerializer.Serialize(catalog.Properties.Value, _serializerOptions);
                            textEntries.Add(new TextEntry($"/{physicalId}", key, value));
                        }

                        long groupId = -1;

                        try
                        {
                            groupId = OpenOrCreateStruct(_fileId, physicalId).GroupId;

                            // file -> catalog -> resources
                            foreach (var catalogItem in catalogItemGroup)
                            {
                                (var chunkLength, var chunkCount) = GeneralHelper.CalculateChunkParameters(totalLength);
                                PrepareResource(groupId, catalogItem, chunkLength, chunkCount);
                            }
                        }
                        finally
                        {
                            if (H5I.is_valid(groupId) > 0) { H5G.close(groupId); }
                        }

                        PrepareAllTextEntries(textEntries);
                    }
                }
                finally
                {
                    if (H5I.is_valid(propertyId) > 0) { H5P.close(propertyId); }

                    H5F.flush(_fileId, H5F.scope_t.GLOBAL);

                    // write preamble
                    if (H5I.is_valid(_fileId) > 0)
                    {
                        H5F.close(_fileId);

                        WritePreamble(filePath);
                    }
                }

                _fileId = H5F.open(filePath, H5F.ACC_RDWR);
            });
        }

        public Task WriteAsync(
            TimeSpan fileOffset,
            WriteRequest[] requests,
            IProgress<double> progress,
            CancellationToken cancellationToken)
        {
            return Task.Run(() =>
            {
                try
                {
                    var offset = (ulong)(fileOffset.Ticks / _lastSamplePeriod.Ticks);

                    var requestGroups = requests
                        .GroupBy(request => request.CatalogItem.Catalog)
                        .ToList();

                    var processed = 0;

                    foreach (var requestGroup in requestGroups)
                    {
                        var catalog = requestGroup.Key;
                        var physicalId = catalog.Id.TrimStart('/').Replace('/', '_');
                        var writeRequests = requestGroup.ToArray();

                        for (int i = 0; i < writeRequests.Length; i++)
                        {
                            cancellationToken.ThrowIfCancellationRequested();
                            WriteData(physicalId, offset, writeRequests[i]);
                        }

                        processed++;
                        progress.Report((double)processed / requests.Length);
                    }
                }
                finally
                {
                    H5F.flush(_fileId, H5F.scope_t.GLOBAL);
                }
            });
        }

        public Task CloseAsync(CancellationToken cancellationToken)
        {
            if (H5I.is_valid(_fileId) > 0) { H5F.close(_fileId); }

            return Task.CompletedTask;
        }

        private unsafe void WriteData(string catalogPhysicalId, ulong fileOffset, WriteRequest writeRequest)
        {
            long groupId = -1;
            long datasetId = -1;
            long dataspaceId = -1;
            long dataspaceId_Buffer = -1;

            try
            {
                var length = (ulong)writeRequest.Data.Length;
                groupId = H5G.open(_fileId, $"/{catalogPhysicalId}/{writeRequest.CatalogItem.Resource.Id}");

                var datasetName = $"dataset_{writeRequest.CatalogItem.Representation.Id}";
                datasetId = H5D.open(groupId, datasetName);
                dataspaceId = H5D.get_space(datasetId);
                dataspaceId_Buffer = H5S.create_simple(1, new ulong[] { length }, null);

                // dataset
                H5S.select_hyperslab(dataspaceId,
                                    H5S.seloper_t.SET,
                                    new ulong[] { fileOffset },
                                    new ulong[] { 1 },
                                    new ulong[] { 1 },
                                    new ulong[] { length });

                fixed (byte* bufferPtr = MemoryMarshal.AsBytes(writeRequest.Data.Span))
                {
                    if (H5D.write(datasetId, H5T.NATIVE_DOUBLE, dataspaceId_Buffer, dataspaceId, H5P.DEFAULT, new IntPtr(bufferPtr)) < 0)
                        throw new Exception(ErrorMessage.Mat73Writer_CouldNotWriteChunk_Dataset);
                }
            }
            finally
            {
                if (H5I.is_valid(groupId) > 0) { H5G.close(groupId); }
                if (H5I.is_valid(datasetId) > 0) { H5D.close(datasetId); }
                if (H5I.is_valid(dataspaceId) > 0) { H5S.close(dataspaceId); }
                if (H5I.is_valid(dataspaceId_Buffer) > 0) { H5S.close(dataspaceId_Buffer); }
            }
        }

        private void PrepareResource(long locationId, CatalogItem catalogItem, ulong chunkLength, ulong chunkCount)
        {
            long groupId = -1;
            long datasetId = -1;

            try
            {
                if (chunkLength <= 0)
                    throw new Exception(ErrorMessage.Mat73Writer_SampleRateTooLow);

                groupId = OpenOrCreateStruct(locationId, catalogItem.Resource.Id).GroupId;
                datasetId = OpenOrCreateResource(groupId, $"dataset_{catalogItem.Representation.Id}", chunkLength, chunkCount).DatasetId;
            }
            finally
            {
                if (H5I.is_valid(groupId) > 0) { H5G.close(groupId); }
                if (H5I.is_valid(datasetId) > 0) { H5D.close(datasetId); }
            }
        }

        // low level
        private (long DatasetId, bool IsNew) OpenOrCreateResource(long locationId, string name, ulong chunkLength, ulong chunkCount)
        {
            long datasetId = -1;
            GCHandle gcHandle_fillValue = default;
            bool isNew;

            try
            {
                var fillValue = Double.NaN;
                gcHandle_fillValue = GCHandle.Alloc(fillValue, GCHandleType.Pinned);

                (datasetId, isNew) = IOHelper.OpenOrCreateDataset(locationId, name, H5T.NATIVE_DOUBLE, chunkLength, chunkCount, gcHandle_fillValue.AddrOfPinnedObject());

                PrepareStringAttribute(datasetId, "MATLAB_class", GetMatTypeFromType(typeof(double)));
            }
            catch (Exception)
            {
                if (H5I.is_valid(datasetId) > 0) { H5D.close(datasetId); }
                
                throw;
            }
            finally
            {
                if (gcHandle_fillValue.IsAllocated)
                    gcHandle_fillValue.Free();
            }

            return (datasetId, isNew);
        }

        private (long GroupId, bool IsNew) OpenOrCreateStruct(long locationId, string path)
        {
            long groupId = -1;
            bool isNew;

            try
            {
                (groupId, isNew) = IOHelper.OpenOrCreateGroup(locationId, path);

                PrepareStringAttribute(groupId, "MATLAB_class", "struct");
            }
            catch (Exception)
            {
                if (H5I.is_valid(groupId) > 0) { H5G.close(groupId); }
                
                throw;
            }

            return (groupId, isNew);
        }

        private void PrepareStringAttribute(long locationId, string name, string value)
        {
            long typeId = -1;
            long attributeId = -1;

            bool isNew;

            try
            {
                var classNamePtr = Marshal.StringToHGlobalAnsi(value);

                typeId = H5T.copy(H5T.C_S1);
                H5T.set_size(typeId, new IntPtr(value.Length));

                (attributeId, isNew) = IOHelper.OpenOrCreateAttribute(locationId, name, typeId, () =>
                {
                    long dataspaceId = -1;
                    long localAttributeId = -1;

                    try
                    {
                        dataspaceId = H5S.create(H5S.class_t.SCALAR);
                        localAttributeId = H5A.create(locationId, name, typeId, dataspaceId);
                    }
                    finally
                    {
                        if (H5I.is_valid(dataspaceId) > 0) { H5S.close(dataspaceId); }
                    }

                    return localAttributeId;
                });

                if (isNew)
                    H5A.write(attributeId, typeId, classNamePtr);
            }
            finally
            {
                if (H5I.is_valid(typeId) > 0) { H5T.close(typeId); }
                if (H5I.is_valid(attributeId) > 0) { H5A.close(attributeId); }
            }
        }

        private void PrepareInt32Attribute(long locationId, string name, Int32 value)
        {
            long attributeId = -1;
            bool isNew;

            try
            {
                (attributeId, isNew) = IOHelper.OpenOrCreateAttribute(locationId, name, H5T.NATIVE_INT32, 1, new ulong[] { 1 });

                if (isNew)
                    IOHelper.Write(attributeId, new Int32[] { value }, DataContainerType.Attribute);
            }
            finally
            {
                if (H5I.is_valid(attributeId) > 0) { H5A.close(attributeId); }
            }
        }

        private string GetMatTypeFromType(Type type)
        {
            if (type == typeof(Double))
                return "double";
            else if (type == typeof(char))
                return "char";
            else if (type == typeof(string))
                return "cell";
            else
                throw new NotImplementedException();
        }

        // low level -> preamble

        private void WritePreamble(string filePath)
        {
            using (FileStream fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Write))
            {
                var streamData1 = Encoding.ASCII.GetBytes($"MATLAB 7.3 MAT-file, Platform: PCWIN64, Created on: { DateTime.Now.ToString("ddd MMM dd HH:mm:ss yyyy", CultureInfo.InvariantCulture) } HDF5 schema 1.00 .                     ");
                var streamData2 = new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x49, 0x4D };
                var streamData3 = new byte[512 - streamData1.Length - streamData2.Length];

                fileStream.Write(streamData1, 0, streamData1.Length);
                fileStream.Write(streamData2, 0, streamData2.Length);
                fileStream.Write(streamData3, 0, streamData3.Length);
            }
        }

        // low level -> string
        private char GetRefsName(byte index)
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

        private void PrepareAllTextEntries(IList<TextEntry> textEntrySet)
        {
            var index = (byte)0;

            textEntrySet.ToList().ForEach(textEntry =>
            {
                long groupId = -1;
                bool isNew;

                try
                {
                    (groupId, isNew) = OpenOrCreateStruct(_fileId, textEntry.Path);
                    PrepareRefsCellString(textEntry, GetRefsName(index).ToString());
                    PrepareTextEntryCellString(textEntry, GetRefsName(index).ToString());
                }
                finally
                {
                    if (H5I.is_valid(groupId) > 0) { H5G.close(groupId); }
                }

                index++;
            });
        }

        private void PrepareRefsCellString(TextEntry textEntry, string refsEntryName)
        {
            long datasetId = -1;
            bool isNew;

            GCHandle gcHandle_data;

            gcHandle_data = default;

            try
            {
                var data = textEntry.Content.ToCodePoints().ToList().ConvertAll(value => (UInt16)value).ToArray();
                gcHandle_data = GCHandle.Alloc(data, GCHandleType.Pinned);

                (datasetId, isNew) = IOHelper.OpenOrCreateDataset(_fileId, $"/#refs#/{ refsEntryName }", H5T.NATIVE_UINT16, () =>
                {
                    long dataspaceId = -1;
                    long lcPropertyId = -1;

                    try
                    {
                        lcPropertyId = H5P.create(H5P.LINK_CREATE);
                        H5P.set_create_intermediate_group(lcPropertyId, 1);
                        dataspaceId = H5S.create_simple(2, new UInt64[] { (UInt64)data.Length, 1 }, null);
                        datasetId = H5D.create(_fileId, $"/#refs#/{ refsEntryName }", H5T.NATIVE_UINT16, dataspaceId, lcPropertyId, H5P.DEFAULT, H5P.DEFAULT);
                    }
                    catch
                    {
                        if (H5I.is_valid(datasetId) > 0) { H5D.close(datasetId); }
                        
                        throw;
                    }
                    finally
                    {
                        if (H5I.is_valid(lcPropertyId) > 0) { H5P.close(lcPropertyId); }
                        if (H5I.is_valid(dataspaceId) > 0) { H5S.close(dataspaceId); }
                    }

                    return datasetId;
                });

                if (isNew)
                    H5D.write(datasetId, H5T.NATIVE_UINT16, H5S.ALL, H5S.ALL, H5P.DEFAULT, gcHandle_data.AddrOfPinnedObject());

                PrepareStringAttribute(datasetId, "MATLAB_class", GetMatTypeFromType(typeof(char)));
                PrepareInt32Attribute(datasetId, "MATLAB_int_decode", 2);
            }
            finally
            {
                if (gcHandle_data.IsAllocated)
                    gcHandle_data.Free();

                if (H5I.is_valid(datasetId) > 0) { H5D.close(datasetId); }
            }
        }

        private void PrepareTextEntryCellString(TextEntry textEntry, string refsEntryName)
        {
            long datasetId = -1;
            bool isNew;

            var objectReferencePointer = IntPtr.Zero;

            try
            {
                objectReferencePointer = Marshal.AllocHGlobal(8);

                (datasetId, isNew) = IOHelper.OpenOrCreateDataset(_fileId, $"{ textEntry.Path }/{ textEntry.Name }", H5T.STD_REF_OBJ, () =>
                {
                    long dataspaceId = -1;

                    try
                    {
                        dataspaceId = H5S.create_simple(2, new UInt64[] { 1, 1 }, null);
                        datasetId = H5D.create(_fileId, $"{ textEntry.Path }/{ textEntry.Name }", H5T.STD_REF_OBJ, dataspaceId);
                    }
                    catch
                    {
                        if (H5I.is_valid(datasetId) > 0) { H5D.close(datasetId); }
                        
                        throw;
                    }
                    finally
                    {
                        if (H5I.is_valid(dataspaceId) > 0) { H5S.close(dataspaceId); }
                    }

                    return datasetId;
                });

                if (isNew)
                {
                    H5R.create(objectReferencePointer, _fileId, $"/#refs#/{ refsEntryName }", H5R.type_t.OBJECT, -1);
                    H5D.write(datasetId, H5T.STD_REF_OBJ, H5S.ALL, H5S.ALL, H5P.DEFAULT, objectReferencePointer);
                }

                PrepareStringAttribute(datasetId, "MATLAB_class", GetMatTypeFromType(typeof(string)));
            }
            finally
            {
                if (objectReferencePointer != IntPtr.Zero)
                    Marshal.FreeHGlobal(objectReferencePointer);

                if (H5I.is_valid(datasetId) > 0) { H5D.close(datasetId); }
            }
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

        #endregion
    }
}