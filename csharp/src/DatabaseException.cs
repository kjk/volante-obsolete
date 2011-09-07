namespace Volante
{
    using System;

    /// <summary>Exception thrown by database implementation
    /// </summary>
    public class DatabaseException : System.ApplicationException
    {
        /// <summary>Get exception error code (see definitions above)
        /// </summary>
        public virtual ErrorCode Code
        {
            get
            {
                return errorCode;
            }
        }

        public virtual Exception OriginalException
        {
            get
            {
                return origEx;
            }
        }

        public enum ErrorCode
        {
            DATABASE_NOT_OPENED,
            DATABASE_ALREADY_OPENED,
            FILE_ACCESS_ERROR,
            KEY_NOT_UNIQUE,
            KEY_NOT_FOUND,
            SCHEMA_CHANGED,
            UNSUPPORTED_TYPE,
            UNSUPPORTED_INDEX_TYPE,
            INCOMPATIBLE_KEY_TYPE,
            INCOMPATIBLE_VALUE_TYPE,
            NOT_ENOUGH_SPACE,
            DATABASE_CORRUPTED,
            CONSTRUCTOR_FAILURE,
            DESCRIPTOR_FAILURE,
            ACCESS_TO_STUB,
            INVALID_OID,
            DELETED_OBJECT,
            ACCESS_VIOLATION,
            CLASS_NOT_FOUND,
            AMBIGUITY_CLASS,
            INDEXED_FIELD_NOT_FOUND
        }

        private static string[] messageText = new string[] {
            "Database not opened",
            "Database already opened",
            "File access error",
            "Key not unique",
            "Key not found",
            "Database schema was changed for",
            "Unsupported type",
            "Unsupported index type",
            "Incompatible key type",
            "Incompatible value type",
            "Not enough space",
            "Database file is corrupted",
            "Failed to instantiate the object of",
            "Failed to build descriptor for",
            "Stub object is accessed",
            "Invalid object reference",
            "Access to the deleted object",
            "Object access violation",
            "Failed to locate",
            "Ambiguity definition of class",
            "Could not find indexed field",
            "No such property",
            "Bad property value"
        };

        /// <summary>Get original exception if DatabaseException was thrown as the result 
        /// of catching some other exception within database implementation. 
        /// DatabaseException is used as a wrapper of other exceptions to avoid cascading
        /// propagation of throw and try/catch.
        /// </summary>
        /// <returns>original exception or <code>null</code> if there was no such exception
        /// 
        /// </returns>
        public DatabaseException(ErrorCode errorCode)
            : base(messageText[(int)errorCode])
        {
            this.errorCode = errorCode;
        }

        public DatabaseException(ErrorCode errorCode, Exception x)
            : base(messageText[(int)errorCode] + ": " + x)
        {
            this.errorCode = errorCode;
            origEx = x;
        }

        public DatabaseException(ErrorCode errorCode, object param)
            : base(messageText[(int)errorCode] + " " + param)
        {
            this.errorCode = errorCode;
        }

        public DatabaseException(ErrorCode errorCode, object param, System.Exception x)
            : base(messageText[(int)errorCode] + " " + param + ": " + x)
        {
            this.errorCode = errorCode;
            origEx = x;
        }

        private ErrorCode errorCode;
        private Exception origEx;
    }
}