using HDF.PInvoke;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Nexus.Writers;

public static class IOHelper
{
    #region "Methods"

    public static void PrepareAttribute<T>(long locationId, string name, T[] valueSet, ulong[] dimensionLimitSet, bool initializeAttribute, [CallerMemberName()] string callerMemberName = "")
    {
        long fileId = -1;
        long typeId = -1;
        long attributeId = -1;

        ulong[] dimensionSet;
        bool isNew;

        Type elementType;

        dimensionSet = [0];
        elementType = typeof(T);

        try
        {
            fileId = H5I.get_file_id(locationId);

            // create attribute
            typeId = TypeConversionHelper.GetHdfTypeIdFromType(fileId, elementType);
            (attributeId, isNew) = IOHelper.OpenOrCreateAttribute(locationId, name, typeId, (ulong)valueSet.LongLength, dimensionLimitSet);

            // write attribute data (regenerate attribute if necessary)
            if (initializeAttribute)
            {
                ulong[] oldValueSetCount;

                if (!isNew && callerMemberName != nameof(IOHelper.PrepareAttribute))
                    oldValueSetCount = IOHelper.PrepareAttributeValueSet(attributeId, ref valueSet, false);
                else
                    oldValueSetCount = [(ulong)valueSet.Length];

                if (valueSet.Length == (int)oldValueSetCount[0])
                {
                    IOHelper.Write(attributeId, valueSet, DataContainerType.Attribute);
                }
                else
                {
                    _ = H5A.close(attributeId);
                    _ = H5A.delete(locationId, name);

                    IOHelper.PrepareAttribute(locationId, name, valueSet, dimensionLimitSet, true);
                }
            }
        }
        finally
        {
            if (H5I.is_valid(attributeId) > 0) { _ = H5A.close(attributeId); }
            if (H5I.is_valid(typeId) > 0) { _ = H5T.close(typeId); }
            if (H5I.is_valid(fileId) > 0) { _ = H5F.close(fileId); }
        }
    }

    public static T[] ReadDataset<T>(long locationId, string datasetPath, ulong start = 0, ulong block = 0)
    {
        long datasetId = -1;
        long dataspaceId = -1;

        T[] result;

        try
        {
            datasetId = H5D.open(locationId, datasetPath);

            if (H5I.is_valid(datasetId) <= 0)
                throw new Exception(ErrorMessage.IOHelper_CouldNotOpenDataset);

            if (start == 0 && block == 0)
            {
                result = IOHelper.Read<T>(datasetId, DataContainerType.Dataset);
            }
            else
            {
                dataspaceId = H5D.get_space(datasetId);

                if (H5S.select_hyperslab(dataspaceId, H5S.seloper_t.SET, [start], [1], [block], [1]) < 0)
                    throw new Exception(ErrorMessage.IOHelper_CouldNotSelectHyperslab);

                result = IOHelper.Read<T>(datasetId, DataContainerType.Dataset, dataspaceId);
            }
        }
        finally
        {
            if (H5I.is_valid(dataspaceId) > 0) { _ = H5S.close(dataspaceId); }
            if (H5I.is_valid(datasetId) > 0) { _ = H5D.close(datasetId); }
        }

        return result;
    }

    public static unsafe T[] Read<T>(long dataPortId, DataContainerType dataContainerType, long dataspaceId = -1)
    {
        long dataspaceId_file = -1;
        long dataspaceId_buffer = -1;
        long typeId = -1;

        long elementCount;

        var elementTypeSize = 0;
        var byteLength = 0;

        var bufferPtr = IntPtr.Zero;
        var elementType = typeof(T);
        var returnValue = default(T[]);

        try
        {
            if (dataspaceId > -1)
                dataspaceId = H5S.copy(dataspaceId);

            switch (dataContainerType)
            {
                case DataContainerType.Attribute:

                    if (dataspaceId == -1)
                        dataspaceId = H5A.get_space(dataPortId);

                    break;

                case DataContainerType.Dataset:

                    if (dataspaceId == -1)
                        dataspaceId = H5D.get_space(dataPortId);

                    dataspaceId_file = dataspaceId;

                    break;

                default:
                    throw new NotSupportedException();
            }

            if (elementType == typeof(string))
                elementTypeSize = Marshal.SizeOf<IntPtr>();
            else if (elementType == typeof(bool))
                elementTypeSize = Marshal.SizeOf<byte>();
            else
                elementTypeSize = Marshal.SizeOf(elementType);

            elementCount = H5S.get_select_npoints(dataspaceId);
            byteLength = (int)elementCount * elementTypeSize;
            bufferPtr = Marshal.AllocHGlobal(byteLength);
            typeId = TypeConversionHelper.GetHdfTypeIdFromType(elementType);

            switch (dataContainerType)
            {
                case DataContainerType.Attribute:

                    if (H5A.read(dataPortId, typeId, bufferPtr) < 0)
                        throw new Exception(ErrorMessage.IOHelper_CouldNotReadAttribute);

                    break;

                case DataContainerType.Dataset:

                    dataspaceId_buffer = H5S.create_simple(1, [(ulong)elementCount], [(ulong)elementCount]);

                    if (H5D.read(dataPortId, typeId, dataspaceId_buffer, dataspaceId_file, H5P.DEFAULT, bufferPtr) < 0)
                        throw new Exception(ErrorMessage.IOHelper_CouldNotReadDataset);

                    break;

                default:
                    throw new NotSupportedException();
            }

            if (elementType.IsPrimitive)
            {
                T[] genericSet;
                GCHandle gcHandle;
                byte[] byteSet;

                genericSet = new T[(int)elementCount];
                gcHandle = GCHandle.Alloc(genericSet, GCHandleType.Pinned);
                byteSet = new byte[byteLength];

                Marshal.Copy(bufferPtr, byteSet, 0, byteLength);
                Marshal.Copy(byteSet, 0, gcHandle.AddrOfPinnedObject(), byteLength);

                returnValue = genericSet;

                gcHandle.Free();
            }
            else if (elementType == typeof(string))
            {
                IntPtr[] intPtrSet;

                intPtrSet = new IntPtr[(int)elementCount];

                Marshal.Copy(bufferPtr, intPtrSet, 0, (int)elementCount);

                returnValue = intPtrSet.Select(x =>
                {
                    // keep this, otherwise °C gets read-in wrongly on Linux 
                    // (https://docs.microsoft.com/en-us/dotnet/api/system.runtime.interopservices.marshal.ptrtostringansi?view=net-5.0)
                    var result = IOHelper.PtrToStringAnsiWithEncoding(x, Encoding.GetEncoding(1252));
                    _ = H5.free_memory(x);
                    return result;
                }).Cast<T>().ToArray();
            }
            else if (elementType.IsValueType && !elementType.IsPrimitive && !elementType.IsEnum)
            {
                T[] structSet;
                int offset;

                structSet = new T[(int)elementCount];
                offset = 0;

                Enumerable.Range(0, (int)elementCount).ToList().ForEach(x =>
                {
                    structSet[x] = Marshal.PtrToStructure<T>(IntPtr.Add(bufferPtr, offset)) ?? throw new Exception("structure is null");
                    offset += elementTypeSize;
                });

                returnValue = structSet;
            }
            else
            {
                throw new NotSupportedException();
            }
        }
        finally
        {
            Marshal.FreeHGlobal(bufferPtr);

            if (H5I.is_valid(typeId) > 0) { _ = H5T.close(typeId); }
            if (H5I.is_valid(dataspaceId_buffer) > 0) { _ = H5S.close(dataspaceId_buffer); }
            if (H5I.is_valid(dataspaceId) > 0) { _ = H5S.close(dataspaceId); }
        }

        return returnValue;
    }

    public static void WriteDataset<T>(long locationId, string datasetName, T[] valueSet)
    {
        long datasetId = -1;

        try
        {
            datasetId = H5D.open(locationId, datasetName);
            IOHelper.Write<T>(datasetId, valueSet, DataContainerType.Dataset);
        }
        finally
        {
            if (H5I.is_valid(datasetId) > 0) { _ = H5D.close(datasetId); }
        }
    }

    public static unsafe void Write<T>(long dataTargetId, T[] valueSet, DataContainerType dataContainerType)
    {
        long typeId = -1;

        int elementTypeSize;
        int byteLength;

        IntPtr valueSetPointer;
        GCHandle gcHandle;
        Type elementType;

        elementTypeSize = 0;
        byteLength = 0;

        valueSetPointer = default;
        gcHandle = default;
        elementType = typeof(T);

        try
        {
            typeId = TypeConversionHelper.GetHdfTypeIdFromType(elementType);

            if (elementType == typeof(string))
                elementTypeSize = Marshal.SizeOf<IntPtr>();
            else if (elementType == typeof(bool))
                elementTypeSize = Marshal.SizeOf<byte>();
            else
                elementTypeSize = Marshal.SizeOf(elementType);

            byteLength = elementTypeSize * valueSet.Length;

            if (elementType.IsPrimitive)
            {
                gcHandle = GCHandle.Alloc(valueSet, GCHandleType.Pinned);
                valueSetPointer = gcHandle.AddrOfPinnedObject();
            }
            else if (elementType == typeof(string))
            {
                var intPtrSet = valueSet.Cast<string>().Select(x => Marshal.StringToHGlobalAnsi(x)).ToArray(); // utf8 ?

                valueSetPointer = Marshal.AllocHGlobal(byteLength);
                Marshal.Copy(intPtrSet, 0, valueSetPointer, intPtrSet.Length);
            }
            else if (elementType.IsValueType && !elementType.IsPrimitive && !elementType.IsEnum)
            {
                var counter = 0;
                valueSetPointer = Marshal.AllocHGlobal(byteLength);

                valueSet.Cast<ValueType>().ToList().ForEach(x =>
                {
                    var sourcePtr = Marshal.AllocHGlobal(elementTypeSize);
                    Marshal.StructureToPtr(x, sourcePtr, false);

                    var source = new Span<byte>(sourcePtr.ToPointer(), Marshal.SizeOf<T>());
                    var target = new Span<byte>(IntPtr.Add(valueSetPointer, elementTypeSize * counter).ToPointer(), Marshal.SizeOf<T>());

                    source.CopyTo(target);

                    counter += 1;
                    Marshal.FreeHGlobal(sourcePtr);
                });
            }
            else
            {
                throw new NotSupportedException();
            }

            switch (dataContainerType)
            {
                case DataContainerType.Attribute:

                    if (H5A.write(dataTargetId, typeId, valueSetPointer) < 0)
                        throw new Exception(ErrorMessage.IOHelper_CouldNotWriteAttribute);

                    break;

                case DataContainerType.Dataset:

                    if (H5D.write(dataTargetId, typeId, H5S.ALL, H5S.ALL, H5P.DEFAULT, valueSetPointer) < 0)
                        throw new Exception(ErrorMessage.IOHelper_CouldNotWriteDataset);

                    break;

                default:
                    throw new NotSupportedException();
            }
        }
        finally
        {
            if (gcHandle.IsAllocated)
                gcHandle.Free();
            else
                Marshal.FreeHGlobal(valueSetPointer);

            if (H5I.is_valid(typeId) > 0) { _ = H5T.close(typeId); }
        }
    }

    public static ulong[] PrepareAttributeValueSet<T>(long attributeId, ref T[] valueSet, bool isReference)
    {
        long dataspaceId = -1;
        var dimensionLimitSet = new ulong[] { 0 };

        try
        {
            dataspaceId = H5A.get_space(attributeId);

            _ = H5S.get_simple_extent_dims(dataspaceId, null, dimensionLimitSet);

            // merge data
            if (dimensionLimitSet[0] == H5S.UNLIMITED)
            {
                var valueSet_File = IOHelper.Read<T>(attributeId, DataContainerType.Attribute);

                if (isReference)
                {
                    if (valueSet_File.Length == 0 || !Enumerable.SequenceEqual(valueSet_File, valueSet.Skip(Math.Max(0, valueSet.Length - valueSet_File.Length))))
                        valueSet = [.. valueSet, .. valueSet_File];
                }
                else
                {
                    if (valueSet.Length == 0 || !Enumerable.SequenceEqual(valueSet, valueSet_File.Skip(Math.Max(0, valueSet_File.Length - valueSet.Length))))
                        valueSet = [.. valueSet_File, .. valueSet];
                }
            }
        }
        finally
        {
            if (H5I.is_valid(dataspaceId) > 0) { _ = H5S.close(dataspaceId); }
        }

        return dimensionLimitSet;
    }

    public static (long AttributeId, bool IsNew) OpenOrCreateAttribute(long locationId, string name, long attributeTypeId, ulong length, ulong[] dimensionLimitSet)
    {
        return IOHelper.OpenOrCreateAttribute(locationId, name, attributeTypeId, () =>
        {
            long dataspaceId = -1;
            long attributeId = -1;

            try
            {
                dataspaceId = H5S.create_simple(1, [length], dimensionLimitSet);
                attributeId = H5A.create(locationId, name, attributeTypeId, dataspaceId);

                if (H5I.is_valid(attributeId) <= 0)
                {
                    throw new Exception(ErrorMessage.IOHelper_CouldNotOpenOrCreateAttribute);
                }
            }
            finally
            {
                if (H5I.is_valid(dataspaceId) > 0) { _ = H5S.close(dataspaceId); }
            }

            return attributeId;
        });
    }

    public static (long AttributeId, bool IsNew) OpenOrCreateAttribute(long locationId, string name, long attributeTypeId, Func<long> createAttributeCallback)
    {
        long attributeId = -1;
        long attributeTypeId_actual = -1;

        bool isNew;

        try
        {
            if (H5A.exists(locationId, name) > 0)
            {
                attributeId = H5A.open(locationId, name);
                attributeTypeId_actual = H5A.get_type(attributeId);

                if (H5T.equal(attributeTypeId_actual, attributeTypeId) <= 0)
                {
                    throw new Exception($"{ErrorMessage.IOHelper_DataTypeMismatch} Attribute: '{name}'.");
                }

                isNew = false;
            }
            else
            {
                attributeId = createAttributeCallback.Invoke();

                isNew = true;
            }

            if (H5I.is_valid(attributeId) <= 0)
            {
                throw new Exception($"{ErrorMessage.IOHelper_CouldNotOpenOrCreateAttribute} Attribute: '{name}'.");
            }
        }
        finally
        {
            if (H5I.is_valid(attributeTypeId_actual) > 0) { _ = H5T.close(attributeTypeId_actual); }
        }

        return (attributeId, isNew);
    }

    public static (long DatasetId, bool IsNew) OpenOrCreateDataset(long locationId, string datasetPath, long datasetTypeId, ulong chunkLength, ulong chunkCount, IntPtr fillValue = default)
    {
        return IOHelper.OpenOrCreateDataset(locationId, datasetPath, datasetTypeId, () =>
        {
            long dcPropertyId = -1;
            long lcPropertyId = -1;
            long dataspaceId = -1;
            long datasetId = -1;

            try
            {
                dcPropertyId = H5P.create(H5P.DATASET_CREATE);

                if (fillValue != IntPtr.Zero)
                {
                    _ = H5P.set_fill_value(dcPropertyId, datasetTypeId, fillValue);
                }

                _ = H5P.set_shuffle(dcPropertyId);
                _ = H5P.set_deflate(dcPropertyId, 7);
                _ = H5P.set_chunk(dcPropertyId, 1, [chunkLength]);

                lcPropertyId = H5P.create(H5P.LINK_CREATE);
                _ = H5P.set_create_intermediate_group(lcPropertyId, 1);

                dataspaceId = H5S.create_simple(1, [chunkLength * chunkCount], null);
                datasetId = H5D.create(locationId, datasetPath, datasetTypeId, dataspaceId, lcPropertyId, dcPropertyId);

                if (H5I.is_valid(datasetId) <= 0)
                {
                    throw new Exception($"{ErrorMessage.IOHelper_CouldNotOpenOrCreateDataset} Dataset: '{datasetPath}'.");
                }
            }
            finally
            {
                if (H5I.is_valid(dcPropertyId) > 0) { _ = H5P.close(dcPropertyId); }
                if (H5I.is_valid(lcPropertyId) > 0) { _ = H5P.close(lcPropertyId); }
                if (H5I.is_valid(dataspaceId) > 0) { _ = H5S.close(dataspaceId); }
            }

            return datasetId;
        });
    }

    public static (long DatasetId, bool IsNew) OpenOrCreateDataset(long locationId, string datasetPath, long datasetTypeId, Func<long> createDatasetCallback)
    {
        long datasetId = -1;
        long datasetTypeId_actual = -1;

        bool isNew;

        try
        {
            if (CheckLinkExists(locationId, datasetPath))
            {
                datasetId = H5D.open(locationId, datasetPath);
                datasetTypeId_actual = H5D.get_type(datasetId);

                if (H5T.equal(datasetTypeId_actual, datasetTypeId) <= 0)
                {
                    throw new Exception($"{ErrorMessage.IOHelper_DataTypeMismatch} Dataset: '{datasetPath}'.");
                }

                isNew = false;
            }
            else
            {
                datasetId = createDatasetCallback.Invoke();

                isNew = true;
            }

            if (H5I.is_valid(datasetId) <= 0)
            {
                throw new Exception($"{ErrorMessage.IOHelper_CouldNotOpenOrCreateDataset} Dataset: '{datasetPath}'.");
            }
        }
        finally
        {
            if (H5I.is_valid(datasetTypeId_actual) > 0) { _ = H5T.close(datasetTypeId_actual); }
        }

        return (datasetId, isNew);
    }

    public static (long GroupId, bool IsNew) OpenOrCreateGroup(long locationId, string groupPath)
    {
        long groupId = -1;
        long propertyId = -1;

        bool isNew;

        try
        {
            if (IOHelper.CheckLinkExists(locationId, groupPath))
            {
                groupId = H5G.open(locationId, groupPath);
                isNew = false;
            }
            else
            {
                propertyId = H5P.create(H5P.LINK_CREATE);
                _ = H5P.set_create_intermediate_group(propertyId, 1);
                groupId = H5G.create(locationId, groupPath, propertyId);
                isNew = true;
            }

            if (H5I.is_valid(groupId) <= 0)
            {
                throw new Exception($"{ErrorMessage.IOHelper_CouldNotOpenOrCreateGroup} Group name: '{groupPath}'.");
            }
        }
        finally
        {
            if (H5I.is_valid(propertyId) > 0) { _ = H5P.close(propertyId); }
        }

        return (groupId, isNew);
    }

    private static readonly char[] separator = ['/'];

    public static bool CheckLinkExists(long locationId, string path)
    {
        string[] path_splitted;
        string currentPath;

        currentPath = ".";
        path_splitted = path.Split(separator, StringSplitOptions.RemoveEmptyEntries);

        for (var i = 0; i < path_splitted.Length; i++)
        {
            currentPath = $"{currentPath}/{path_splitted[i]}";

            if (H5L.exists(locationId, currentPath) == 0)
            {
                return false;
            }
        }

        return true;
    }

    public static unsafe string PtrToStringAnsiWithEncoding(IntPtr ptr, Encoding encoding)
    {
        var l = 0;
        var bytePtr = (byte*)ptr.ToPointer();

        while (bytePtr[l] != 0)
        {
            l++;
        }

        if (l > 0)
        {
            var chars = stackalloc char[l];

            encoding
                .GetDecoder()
                .Convert(bytePtr, l, chars, l, true, out var bytesUsed, out var charsUsed, out var completed);

            return new string(chars, 0, charsUsed);
        }
        else
        {
            return string.Empty;
        }
    }

    #endregion
}
