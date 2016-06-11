package com.inferwerx.ogliabilitytracker.exceptions

// Use this exception when calling a bunch of async calls in a loop
class MultiAsyncException(val errorCollection : Collection<Throwable>) : Throwable() {
    override val message: String?
        get() {
            if (errorCollection.count() == 0) {
                return super.message
            } else if (errorCollection.count() == 1) {
                return errorCollection.elementAt(0).message
            } else {
                var buffer = StringBuffer()

                buffer.append("Multiple errors: ")

                errorCollection.forEach {
                    buffer.append(" [${it.message}] ")
                }

                return buffer.toString()
            }
        }
}