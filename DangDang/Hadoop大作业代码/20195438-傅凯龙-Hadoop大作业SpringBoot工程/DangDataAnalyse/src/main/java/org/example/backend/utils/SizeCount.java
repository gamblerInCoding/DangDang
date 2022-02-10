package org.example.backend.utils;

public class SizeCount {
    public String filesizecount(long filelong){
        int unitlong = 1024;
        if (filelong / unitlong / unitlong / unitlong > 0) {
            return filelong / unitlong / unitlong / unitlong + "GB";
        } else if (filelong / unitlong / unitlong > 0) {
            return filelong / unitlong / unitlong + "MB";
        } else if (filelong / unitlong > 0) {
            return filelong / unitlong + "KB";
        } else {
            return filelong + "B";
        }
    }
}
