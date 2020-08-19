package ru.inno.stc;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteServices;
import org.apache.ignite.Ignition;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteReducer;
import org.apache.ignite.resources.ServiceResource;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.inno.stc.entity.Course;
import ru.inno.stc.services.SimpleMapService;
import ru.inno.stc.services.SimpleMapServiceImpl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Hello world!
 */
public class App {
    private static final Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        try (Ignite ignite = Ignition.start()) {
            IgniteServices services = ignite.services(ignite.cluster().forServers());
            try {
                services.deployClusterSingleton("clusterSingleton", new SimpleMapServiceImpl<Integer, Course>());
                services.deployNodeSingleton("nodeSingleton", new SimpleMapServiceImpl<Integer, Course>());
                services.deployMultiple("multiple", new SimpleMapServiceImpl<Integer, Course>(), 2, 0);

                proxyRun(ignite);
                broadcast(ignite);
                reduse(ignite);
                gatherSystemInfo(ignite);
            } finally {
                ignite.services().cancelAll();
            }

        }
    }

    private static void proxyRun(Ignite ignite) {
        logger.info("Broadcast run");
        SimpleMapService<Integer, Course> sms = ignite.services().serviceProxy("clusterSingleton", SimpleMapService.class, true);

        for (int i = 0; i < 10; i++) {
            sms.put(i, new Course("Course" + i));
        }

        final int size = sms.size();
        if (size != 10) {
            throw new RuntimeException("Invalid map size.");
        }
        logger.info("Map size: {}", size);
    }


    private static void broadcast(Ignite ignite) {
        logger.info("Proxy run");
        SimpleMapService<Integer, Course> sms = ignite.services().serviceProxy("multiple", SimpleMapService.class, true);

        for (int i = 0; i < 10; i++) {
            sms.put(i, new Course("Course" + i));
        }


        final Collection<Integer> sizes = ignite.compute().broadcast(new IgniteCallable<Integer>() {
            @ServiceResource(serviceName = "clusterSingleton", proxyInterface = SimpleMapService.class)
            SimpleMapService<Integer, Course> sms;

            @Override
            public Integer call() {
                return sms.size();
            }
        });

        for (Integer size : sizes) {
            if (size != 10) {
                throw new RuntimeException("Неверный размер.");
            }
            logger.info("Map size: {}", size);
        }
    }


    private static void  reduse(Ignite ignite) {
        logger.info("Reduse run");
        IgniteClosure<Course, Integer> map = (IgniteClosure<Course, Integer>) course -> {
            final int length = course.getTitle().length();
            logger.info("Ситаем {}, количество букв = {}", course, length);
            return length;
        };

        IgniteReducer<Integer, Integer> reduse = new IgniteReducer<Integer, Integer>() {
            private final AtomicInteger sum = new AtomicInteger();

            @Override
            public boolean collect(@Nullable Integer integer) {
                if (integer != null) {
                    sum.addAndGet(integer);
                    return true;
                }
                return false;
            }

            @Override
            public Integer reduce() {
                return sum.get();
            }
        };

        Collection<Course> collection = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            collection.add(new Course("Course" + i));
        }


        final Integer sum = ignite.compute().apply(map, collection, reduse);
        logger.info("Суммарное количество букв = {}", sum);
    }


    private static void gatherSystemInfo(Ignite ignite)  {
        Collection<String> res = ignite.compute().broadcast(() -> {
            System.out.println();
            System.out.println("Executing task on node: " + ignite.cluster().localNode().id());

            return "Node ID: " + ignite.cluster().localNode().id() + "\n" +
                   "OS: " + System.getProperty("os.name") + " " + System.getProperty("os.version") + " " +
                   System.getProperty("os.arch") + "\n" +
                   "User: " + System.getProperty("user.name") + "\n" +
                   "JRE: " + System.getProperty("java.runtime.name") + " " +
                   System.getProperty("java.runtime.version");
        });

        // Print result.
        System.out.println();
        System.out.println("Nodes system information:");
        System.out.println();

        res.forEach(r -> {
            System.out.println(r);
            System.out.println();
        });
    }

}
