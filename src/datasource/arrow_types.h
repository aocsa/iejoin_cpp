#pragma once

// Define ArrowType enum
namespace datafusionx {
    enum class ArrowType { 
        INT32,   // 32-bit integer
        INT64,   // 64-bit integer
        FLOAT,   // Single precision floating point number
        DOUBLE,  // Double precision floating point number
        STRING,  // UTF-8 encoded string
        BOOL     // Boolean value
    };
    
}