using System;
using System.IO;
using System.Diagnostics;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading;
using System.Globalization;

namespace Volante.Impl
{
    internal class CodeGenerator
    {
        public GeneratedSerializer Generate(ClassDescriptor desc)
        {
            ModuleBuilder module = EmitAssemblyModule();
            Type newCls = EmitClass(module, desc);
            return (GeneratedSerializer)module.Assembly.CreateInstance(newCls.Name);
        }

        public Type CreateWrapper(Type type)
        {
            return EmitClassWrapper(EmitAssemblyModule(), type);
        }

        private ModuleBuilder EmitAssemblyModule()
        {
            if (dynamicModule == null)
            {
                AssemblyName assemblyName = new AssemblyName();
                assemblyName.Name = "GeneratedSerializerAssembly";
                //Create a new assembly with one module
                AssemblyBuilder assembly = Thread.GetDomain().DefineDynamicAssembly(assemblyName, AssemblyBuilderAccess.Run);
                dynamicModule = assembly.DefineDynamicModule("GeneratedSerializerModule");
            }
            return dynamicModule;
        }

        private MethodBuilder GetBuilder(TypeBuilder serializerType, MethodInfo methodInterface)
        {
            Type returnType = methodInterface.ReturnType;
            ParameterInfo[] methodParams = methodInterface.GetParameters();
            Type[] paramTypes = new Type[methodParams.Length];
            for (int i = 0; i < methodParams.Length; i++)
            {
                paramTypes[i] = methodParams[i].ParameterType;
            }
            return serializerType.DefineMethod(methodInterface.Name,
                MethodAttributes.Public | MethodAttributes.Virtual,
                returnType,
                paramTypes);
        }

        private void generatePackField(ILGenerator il, FieldInfo f, MethodInfo pack)
        {
            il.Emit(OpCodes.Ldarg_3); // buf
            il.Emit(OpCodes.Ldloc_1, offs); // offs
            il.Emit(OpCodes.Ldloc_0, obj);
            il.Emit(OpCodes.Ldfld, f);
            il.Emit(OpCodes.Call, pack);
            il.Emit(OpCodes.Stloc_1, offs); // offs
        }

        private void generatePackMethod(ClassDescriptor desc, MethodBuilder builder)
        {
            ILGenerator il = builder.GetILGenerator();
            il.Emit(OpCodes.Ldarg_2); // obj
            il.Emit(OpCodes.Castclass, desc.cls);
            obj = il.DeclareLocal(desc.cls);
            il.Emit(OpCodes.Stloc_0, obj);
            il.Emit(OpCodes.Ldc_I4, ObjectHeader.Sizeof);
            offs = il.DeclareLocal(typeof(int));
            il.Emit(OpCodes.Stloc_1, offs);

            ClassDescriptor.FieldDescriptor[] flds = desc.allFields;

            for (int i = 0, n = flds.Length; i < n; i++)
            {
                ClassDescriptor.FieldDescriptor fd = flds[i];
                FieldInfo f = fd.field;
                switch (fd.type)
                {
                    case ClassDescriptor.FieldType.tpByte:
                    case ClassDescriptor.FieldType.tpSByte:
                        generatePackField(il, f, packI1);
                        continue;

                    case ClassDescriptor.FieldType.tpBoolean:
                        generatePackField(il, f, packBool);
                        continue;

                    case ClassDescriptor.FieldType.tpShort:
                    case ClassDescriptor.FieldType.tpUShort:
                    case ClassDescriptor.FieldType.tpChar:
                        generatePackField(il, f, packI2);
                        continue;

                    case ClassDescriptor.FieldType.tpEnum:
                    case ClassDescriptor.FieldType.tpInt:
                    case ClassDescriptor.FieldType.tpUInt:
                        generatePackField(il, f, packI4);
                        continue;

                    case ClassDescriptor.FieldType.tpLong:
                    case ClassDescriptor.FieldType.tpULong:
                        generatePackField(il, f, packI8);
                        continue;

                    case ClassDescriptor.FieldType.tpFloat:
                        generatePackField(il, f, packF4);
                        continue;

                    case ClassDescriptor.FieldType.tpDouble:
                        generatePackField(il, f, packF8);
                        continue;

                    case ClassDescriptor.FieldType.tpDecimal:
                        generatePackField(il, f, packDecimal);
                        continue;

                    case ClassDescriptor.FieldType.tpGuid:
                        generatePackField(il, f, packGuid);
                        continue;

                    case ClassDescriptor.FieldType.tpDate:
                        generatePackField(il, f, packDate);
                        continue;

                    case ClassDescriptor.FieldType.tpString:
                        generatePackField(il, f, packString);
                        continue;

                    default:
                        il.Emit(OpCodes.Ldarg_1); // db
                        il.Emit(OpCodes.Ldarg_3); // buf
                        il.Emit(OpCodes.Ldloc_1, offs);
                        il.Emit(OpCodes.Ldloc_0, obj);
                        il.Emit(OpCodes.Ldfld, f);
                        il.Emit(OpCodes.Ldnull); // fd
                        il.Emit(OpCodes.Ldc_I4, (int)fd.type);
                        il.Emit(OpCodes.Ldloc_0, obj);
                        il.Emit(OpCodes.Call, packField);
                        il.Emit(OpCodes.Stloc_1, offs);
                        continue;
                }
            }
            il.Emit(OpCodes.Ldloc_1, offs);
            il.Emit(OpCodes.Ret);
        }

        private void generateUnpackMethod(ClassDescriptor desc, MethodBuilder builder)
        {
            ILGenerator il = builder.GetILGenerator();
            il.Emit(OpCodes.Ldarg_2);
            il.Emit(OpCodes.Castclass, desc.cls);
            LocalBuilder obj = il.DeclareLocal(desc.cls);
            il.Emit(OpCodes.Stloc_0, obj);
            il.Emit(OpCodes.Ldc_I4, ObjectHeader.Sizeof);
            LocalBuilder offs = il.DeclareLocal(typeof(int));
            il.Emit(OpCodes.Stloc_1, offs);
            LocalBuilder val = il.DeclareLocal(typeof(object));

            ClassDescriptor.FieldDescriptor[] flds = desc.allFields;

            for (int i = 0, n = flds.Length; i < n; i++)
            {
                ClassDescriptor.FieldDescriptor fd = flds[i];
                FieldInfo f = fd.field;
                if (f == null)
                {
                    switch (fd.type)
                    {
                        case ClassDescriptor.FieldType.tpByte:
                        case ClassDescriptor.FieldType.tpSByte:
                        case ClassDescriptor.FieldType.tpBoolean:
                            il.Emit(OpCodes.Ldloc_1, offs);
                            il.Emit(OpCodes.Ldc_I4_1);
                            il.Emit(OpCodes.Add);
                            il.Emit(OpCodes.Stloc_1, offs);
                            continue;

                        case ClassDescriptor.FieldType.tpShort:
                        case ClassDescriptor.FieldType.tpUShort:
                        case ClassDescriptor.FieldType.tpChar:
                            il.Emit(OpCodes.Ldloc_1, offs);
                            il.Emit(OpCodes.Ldc_I4_2);
                            il.Emit(OpCodes.Add);
                            il.Emit(OpCodes.Stloc_1, offs);
                            continue;

                        case ClassDescriptor.FieldType.tpEnum:
                        case ClassDescriptor.FieldType.tpInt:
                        case ClassDescriptor.FieldType.tpUInt:
                        case ClassDescriptor.FieldType.tpFloat:
                            il.Emit(OpCodes.Ldloc_1, offs);
                            il.Emit(OpCodes.Ldc_I4_4);
                            il.Emit(OpCodes.Add);
                            il.Emit(OpCodes.Stloc_1, offs);
                            continue;

                        case ClassDescriptor.FieldType.tpLong:
                        case ClassDescriptor.FieldType.tpULong:
                        case ClassDescriptor.FieldType.tpDate:
                        case ClassDescriptor.FieldType.tpDouble:
                            il.Emit(OpCodes.Ldloc_1, offs);
                            il.Emit(OpCodes.Ldc_I4_8);
                            il.Emit(OpCodes.Add);
                            il.Emit(OpCodes.Stloc_1, offs);
                            continue;

                        case ClassDescriptor.FieldType.tpDecimal:
                        case ClassDescriptor.FieldType.tpGuid:
                            il.Emit(OpCodes.Ldloc_1, offs);
                            il.Emit(OpCodes.Ldc_I4, 16);
                            il.Emit(OpCodes.Add);
                            il.Emit(OpCodes.Stloc_1, offs);
                            continue;

                        default:
                            il.Emit(OpCodes.Ldarg_1); // db
                            il.Emit(OpCodes.Ldarg_3); // body
                            il.Emit(OpCodes.Ldloc_1, offs);
                            il.Emit(OpCodes.Ldnull); // fd
                            il.Emit(OpCodes.Ldc_I4, (int)fd.type);
                            il.Emit(OpCodes.Call, skipField);
                            il.Emit(OpCodes.Stloc_1, offs); // offs
                            continue;
                    }
                }
                else
                {
                    switch (fd.type)
                    {
                        case ClassDescriptor.FieldType.tpByte:
                            il.Emit(OpCodes.Ldloc_0, obj);
                            il.Emit(OpCodes.Ldarg_3); // body
                            il.Emit(OpCodes.Ldloc_1, offs);
                            il.Emit(OpCodes.Ldelem_U1);
                            il.Emit(OpCodes.Stfld, f);
                            il.Emit(OpCodes.Ldloc_1, offs);
                            il.Emit(OpCodes.Ldc_I4_1);
                            il.Emit(OpCodes.Add);
                            il.Emit(OpCodes.Stloc_1, offs);
                            continue;

                        case ClassDescriptor.FieldType.tpSByte:
                            il.Emit(OpCodes.Ldloc_0, obj);
                            il.Emit(OpCodes.Ldarg_3); // body
                            il.Emit(OpCodes.Ldloc_1, offs);
                            il.Emit(OpCodes.Ldelem_U1);
                            il.Emit(OpCodes.Conv_I1);
                            il.Emit(OpCodes.Stfld, f);
                            il.Emit(OpCodes.Ldloc_1, offs);
                            il.Emit(OpCodes.Ldc_I4_1);
                            il.Emit(OpCodes.Add);
                            il.Emit(OpCodes.Stloc_1, offs);
                            continue;

                        case ClassDescriptor.FieldType.tpBoolean:
                            il.Emit(OpCodes.Ldloc_0, obj);
                            il.Emit(OpCodes.Ldarg_3); // body
                            il.Emit(OpCodes.Ldloc_1, offs);
                            il.Emit(OpCodes.Ldelem_U1);
                            il.Emit(OpCodes.Stfld, f);
                            il.Emit(OpCodes.Ldloc_1, offs);
                            il.Emit(OpCodes.Ldc_I4_1);
                            il.Emit(OpCodes.Add);
                            il.Emit(OpCodes.Stloc_1, offs);
                            continue;

                        case ClassDescriptor.FieldType.tpShort:
                            il.Emit(OpCodes.Ldloc_0, obj);
                            il.Emit(OpCodes.Ldarg_3); // body
                            il.Emit(OpCodes.Ldloc_1, offs);
                            il.Emit(OpCodes.Call, unpackI2);
                            il.Emit(OpCodes.Stfld, f);
                            il.Emit(OpCodes.Ldloc_1, offs);
                            il.Emit(OpCodes.Ldc_I4_2);
                            il.Emit(OpCodes.Add);
                            il.Emit(OpCodes.Stloc_1, offs);
                            continue;

                        case ClassDescriptor.FieldType.tpUShort:
                        case ClassDescriptor.FieldType.tpChar:
                            il.Emit(OpCodes.Ldloc_0, obj);
                            il.Emit(OpCodes.Ldarg_3); // body
                            il.Emit(OpCodes.Ldloc_1, offs);
                            il.Emit(OpCodes.Call, unpackI2);
                            il.Emit(OpCodes.Conv_U2);
                            il.Emit(OpCodes.Stfld, f);
                            il.Emit(OpCodes.Ldloc_1, offs);
                            il.Emit(OpCodes.Ldc_I4_2);
                            il.Emit(OpCodes.Add);
                            il.Emit(OpCodes.Stloc_1, offs);
                            continue;

                        case ClassDescriptor.FieldType.tpEnum:
                        case ClassDescriptor.FieldType.tpInt:
                        case ClassDescriptor.FieldType.tpUInt:
                            il.Emit(OpCodes.Ldloc_0, obj);
                            il.Emit(OpCodes.Ldarg_3); // body
                            il.Emit(OpCodes.Ldloc_1, offs);
                            il.Emit(OpCodes.Call, unpackI4);
                            il.Emit(OpCodes.Stfld, f);
                            il.Emit(OpCodes.Ldloc_1, offs);
                            il.Emit(OpCodes.Ldc_I4_4);
                            il.Emit(OpCodes.Add);
                            il.Emit(OpCodes.Stloc_1, offs);
                            continue;

                        case ClassDescriptor.FieldType.tpLong:
                        case ClassDescriptor.FieldType.tpULong:
                            il.Emit(OpCodes.Ldloc_0, obj);
                            il.Emit(OpCodes.Ldarg_3); // body
                            il.Emit(OpCodes.Ldloc_1, offs);
                            il.Emit(OpCodes.Call, unpackI8);
                            il.Emit(OpCodes.Stfld, f);
                            il.Emit(OpCodes.Ldloc_1, offs);
                            il.Emit(OpCodes.Ldc_I4_8);
                            il.Emit(OpCodes.Add);
                            il.Emit(OpCodes.Stloc_1, offs);
                            continue;

                        case ClassDescriptor.FieldType.tpFloat:
                            il.Emit(OpCodes.Ldloc_0, obj);
                            il.Emit(OpCodes.Ldarg_3); // body
                            il.Emit(OpCodes.Ldloc_1, offs);
                            il.Emit(OpCodes.Call, unpackF4);
                            il.Emit(OpCodes.Stfld, f);
                            il.Emit(OpCodes.Ldloc_1, offs);
                            il.Emit(OpCodes.Ldc_I4_4);
                            il.Emit(OpCodes.Add);
                            il.Emit(OpCodes.Stloc_1, offs);
                            continue;

                        case ClassDescriptor.FieldType.tpDouble:
                            il.Emit(OpCodes.Ldloc_0, obj);
                            il.Emit(OpCodes.Ldarg_3); // body
                            il.Emit(OpCodes.Ldloc_1, offs);
                            il.Emit(OpCodes.Call, unpackF8);
                            il.Emit(OpCodes.Stfld, f);
                            il.Emit(OpCodes.Ldloc_1, offs);
                            il.Emit(OpCodes.Ldc_I4_8);
                            il.Emit(OpCodes.Add);
                            il.Emit(OpCodes.Stloc_1, offs);
                            continue;

                        case ClassDescriptor.FieldType.tpDecimal:
                            il.Emit(OpCodes.Ldloc_0, obj);
                            il.Emit(OpCodes.Ldarg_3); // body
                            il.Emit(OpCodes.Ldloc_1, offs);
                            il.Emit(OpCodes.Call, unpackDecimal);
                            il.Emit(OpCodes.Stfld, f);
                            il.Emit(OpCodes.Ldloc_1, offs);
                            il.Emit(OpCodes.Ldc_I4, 16);
                            il.Emit(OpCodes.Add);
                            il.Emit(OpCodes.Stloc_1, offs);
                            continue;

                        case ClassDescriptor.FieldType.tpGuid:
                            il.Emit(OpCodes.Ldloc_0, obj);
                            il.Emit(OpCodes.Ldarg_3); // body
                            il.Emit(OpCodes.Ldloc_1, offs);
                            il.Emit(OpCodes.Call, unpackGuid);
                            il.Emit(OpCodes.Stfld, f);
                            il.Emit(OpCodes.Ldloc_1, offs);
                            il.Emit(OpCodes.Ldc_I4, 16);
                            il.Emit(OpCodes.Add);
                            il.Emit(OpCodes.Stloc_1, offs);
                            continue;

                        case ClassDescriptor.FieldType.tpDate:
                            il.Emit(OpCodes.Ldloc_0, obj);
                            il.Emit(OpCodes.Ldarg_3); // body
                            il.Emit(OpCodes.Ldloc_1, offs);
                            il.Emit(OpCodes.Call, unpackDate);
                            il.Emit(OpCodes.Stfld, f);
                            il.Emit(OpCodes.Ldloc_1, offs);
                            il.Emit(OpCodes.Ldc_I4_8);
                            il.Emit(OpCodes.Add);
                            il.Emit(OpCodes.Stloc_1, offs);
                            continue;

                        case ClassDescriptor.FieldType.tpString:
                            il.Emit(OpCodes.Ldarg_3); // body
                            il.Emit(OpCodes.Ldloc_1, offs);
                            il.Emit(OpCodes.Ldloc_0, obj);
                            il.Emit(OpCodes.Ldflda, f);
                            il.Emit(OpCodes.Call, unpackString);
                            il.Emit(OpCodes.Stloc_1, offs);
                            continue;

                        default:
                            il.Emit(OpCodes.Ldarg_1); // db
                            il.Emit(OpCodes.Ldarg_3); // body
                            il.Emit(OpCodes.Ldloc_1, offs);
                            il.Emit(OpCodes.Ldarg_S, 4); // recursiveLoading
                            il.Emit(OpCodes.Ldloca, val);
                            il.Emit(OpCodes.Ldnull); // fd
                            il.Emit(OpCodes.Ldc_I4, (int)fd.type);
                            il.Emit(OpCodes.Ldloc_0, obj);
                            il.Emit(OpCodes.Call, unpackField);
                            il.Emit(OpCodes.Stloc_1, offs); // offs
                            il.Emit(OpCodes.Ldloc_0, obj);
                            il.Emit(OpCodes.Ldloc, val);
                            il.Emit(OpCodes.Castclass, f.FieldType);
                            il.Emit(OpCodes.Stfld, f);
                            continue;
                    }
                }
            }
            il.Emit(OpCodes.Ret);
        }

        private void generateNewMethod(ClassDescriptor desc, MethodBuilder builder)
        {
            ILGenerator il = builder.GetILGenerator();
            il.Emit(OpCodes.Newobj, desc.defaultConstructor);
            il.Emit(OpCodes.Ret);
        }

        private Type EmitClass(ModuleBuilder module, ClassDescriptor desc)
        {
            counter += 1;
            String generatedClassName = "GeneratedSerializerClass" + counter;
            TypeBuilder serializerType = module.DefineType(generatedClassName, TypeAttributes.Public);

            Type serializerInterface = typeof(GeneratedSerializer);
            serializerType.AddInterfaceImplementation(serializerInterface);
            //Add a constructor
            //TODO: wasn't used, figure out if was needed
            //ConstructorBuilder constructor =
            //    serializerType.DefineDefaultConstructor(MethodAttributes.Public);

            MethodInfo packInterface = serializerInterface.GetMethod("pack");
            MethodBuilder packBuilder = GetBuilder(serializerType, packInterface);
            generatePackMethod(desc, packBuilder);
            serializerType.DefineMethodOverride(packBuilder, packInterface);

            MethodInfo unpackInterface = serializerInterface.GetMethod("unpack");
            MethodBuilder unpackBuilder = GetBuilder(serializerType, unpackInterface);
            generateUnpackMethod(desc, unpackBuilder);
            serializerType.DefineMethodOverride(unpackBuilder, unpackInterface);

            MethodInfo newInterface = serializerInterface.GetMethod("newInstance");
            MethodBuilder newBuilder = GetBuilder(serializerType, newInterface);
            generateNewMethod(desc, newBuilder);
            serializerType.DefineMethodOverride(newBuilder, newInterface);

            serializerType.CreateType();
            return serializerType;
        }

        private Type EmitClassWrapper(ModuleBuilder module, Type type)
        {
            String generatedClassName = type.Name + "Wrapper";
            TypeBuilder wrapperType;

            if (type.IsInterface)
            {
                wrapperType = module.DefineType(generatedClassName, TypeAttributes.Public,
                    typeof(IResource).IsAssignableFrom(type) ? typeof(PersistentResource) : typeof(Persistent));
                wrapperType.AddInterfaceImplementation(type);
            }
            else
            {
                wrapperType = module.DefineType(generatedClassName, TypeAttributes.Public, type);
            }
            wrapperType.AddInterfaceImplementation(typeof(PersistentWrapper));

            //Add a constructor
            //TODO: wasn't used, figure out if was needed
            //ConstructorBuilder constructor =
            //    wrapperType.DefineDefaultConstructor(MethodAttributes.Public);

            PropertyInfo[] properties = type.GetProperties(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
            for (int i = 0; i < properties.Length; i++)
            {
                PropertyInfo prop = properties[i];
                MethodInfo getter = prop.GetGetMethod(true);
                MethodInfo setter = prop.GetSetMethod(true);
                if (getter != null && setter != null && getter.IsVirtual && setter.IsVirtual)
                {
                    Type returnType = getter.ReturnType;
                    String fieldName = prop.Name;
                    Type fieldType;
                    if (typeof(IPersistent).IsAssignableFrom(returnType))
                    {
                        fieldType = typeof(int);
                        fieldName = "r_" + fieldName;
                    }
                    else
                    {
                        fieldType = returnType;
                        fieldName = "s_" + fieldName;
                    }

                    if (fieldType.IsArray && typeof(IPersistent).IsAssignableFrom(fieldType.GetElementType()))
                        throw new DatabaseException(DatabaseException.ErrorCode.UNSUPPORTED_TYPE);

                    FieldBuilder fb = wrapperType.DefineField(fieldName, fieldType, FieldAttributes.Private);

                    MethodBuilder getterImpl = wrapperType.DefineMethod(getter.Name,
                        MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.NewSlot,
                        returnType,
                        new Type[] { });

                    ILGenerator il = getterImpl.GetILGenerator();

                    if (fieldType != returnType)
                    {
                        il.Emit(OpCodes.Ldarg_0);
                        il.Emit(OpCodes.Callvirt, getDatabase);
                        il.Emit(OpCodes.Ldarg_0);
                        il.Emit(OpCodes.Ldfld, fb);
                        il.Emit(OpCodes.Callvirt, getByOid);
                        il.Emit(OpCodes.Castclass, returnType);
                    }
                    else
                    {
                        il.Emit(OpCodes.Ldarg_0);
                        il.Emit(OpCodes.Ldfld, fb);
                    }
                    il.Emit(OpCodes.Ret);

                    wrapperType.DefineMethodOverride(getterImpl, getter);

                    MethodBuilder setterImpl = wrapperType.DefineMethod(setter.Name,
                        MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.NewSlot,
                        null,
                        new Type[] { returnType });

                    il = setterImpl.GetILGenerator();

                    il.Emit(OpCodes.Ldarg_0);
                    if (fieldType != returnType)
                    {
                        il.Emit(OpCodes.Ldarg_0);
                        il.Emit(OpCodes.Callvirt, getDatabase);
                        il.Emit(OpCodes.Ldarg_1);
                        il.Emit(OpCodes.Callvirt, makePersistent);
                    }
                    else
                    {
                        il.Emit(OpCodes.Ldarg_1);
                    }
                    il.Emit(OpCodes.Stfld, fb);
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Callvirt, modify);
                    il.Emit(OpCodes.Ret);

                    wrapperType.DefineMethodOverride(setterImpl, setter);
                }
            }
            wrapperType.CreateType();
            return wrapperType;
        }

        public static CodeGenerator Instance
        {
            get
            {
                if (instance == null)
                    instance = new CodeGenerator();
                return instance;
            }
        }

        private static CodeGenerator instance;

        private LocalBuilder obj;
        private LocalBuilder offs;

        private MethodInfo packBool = typeof(ByteBuffer).GetMethod("packBool");
        private MethodInfo packI1 = typeof(ByteBuffer).GetMethod("packI1");
        private MethodInfo packI2 = typeof(ByteBuffer).GetMethod("packI2");
        private MethodInfo packI4 = typeof(ByteBuffer).GetMethod("packI4");
        private MethodInfo packI8 = typeof(ByteBuffer).GetMethod("packI8");
        private MethodInfo packF4 = typeof(ByteBuffer).GetMethod("packF4");
        private MethodInfo packF8 = typeof(ByteBuffer).GetMethod("packF8");
        private MethodInfo packDecimal = typeof(ByteBuffer).GetMethod("packDecimal");
        private MethodInfo packGuid = typeof(ByteBuffer).GetMethod("packGuid");
        private MethodInfo packDate = typeof(ByteBuffer).GetMethod("packDate");
        private MethodInfo packString = typeof(ByteBuffer).GetMethod("packString");
        private MethodInfo packField = typeof(DatabaseImpl).GetMethod("packField");

        private MethodInfo unpackI2 = typeof(Bytes).GetMethod("unpack2");
        private MethodInfo unpackI4 = typeof(Bytes).GetMethod("unpack4");
        private MethodInfo unpackI8 = typeof(Bytes).GetMethod("unpack8");
        private MethodInfo unpackF4 = typeof(Bytes).GetMethod("unpackF4");
        private MethodInfo unpackF8 = typeof(Bytes).GetMethod("unpackF8");
        private MethodInfo unpackDecimal = typeof(Bytes).GetMethod("unpackDecimal");
        private MethodInfo unpackGuid = typeof(Bytes).GetMethod("unpackGuid");
        private MethodInfo unpackDate = typeof(Bytes).GetMethod("unpackDate");
        private MethodInfo unpackString = typeof(Bytes).GetMethod("unpackString");
        private MethodInfo unpackField = typeof(DatabaseImpl).GetMethod("unpackField");
        private MethodInfo skipField = typeof(DatabaseImpl).GetMethod("skipField");

        private MethodInfo modify = typeof(IPersistent).GetMethod("Modify");
        private MethodInfo getDatabase = typeof(IPersistent).GetProperty("Database").GetGetMethod();
        private MethodInfo getByOid = typeof(IDatabase).GetMethod("GetObjectByOid");
        private MethodInfo makePersistent = typeof(IDatabase).GetMethod("MakePersistent");

        private ModuleBuilder dynamicModule;
        private int counter;
    }
}

