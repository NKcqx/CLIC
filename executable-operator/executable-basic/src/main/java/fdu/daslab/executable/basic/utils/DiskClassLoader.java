package fdu.daslab.executable.basic.utils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;

/**
 * 从本地加载文件成class对象的classLoader
 *
 * @author 唐志伟
 * @since 2020/7/6 1:36 PM
 * @version 1.0
 */
public class DiskClassLoader extends ClassLoader {
    private String mLibPath;

    public DiskClassLoader(String path) {
        mLibPath = path;
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        File file = new File(mLibPath);

        try {
            FileInputStream is = new FileInputStream(file);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            int len;
            try {
                while ((len = is.read()) != -1) {
                    bos.write(len);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            byte[] data = bos.toByteArray();
            is.close();
            bos.close();

            return defineClass(name, data, 0, data.length);

        } catch (IOException e) {
            e.printStackTrace();
        }

        return super.findClass(name);
    }


    @Override
    public URL getResource(String name) {
        return super.getResource(name);
    }
}
