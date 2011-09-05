namespace Volante.Impl
{
    using System;
    using System.Collections;
    using Volante;
    using System.Diagnostics;
    using Link = Volante.ILink<IPersistent>;

    class RtreeR2Page : Persistent
    {
        const int card = (Page.pageSize - ObjectHeader.Sizeof - 4 * 3) / (8 * 4 + 4);
        const int minFill = card / 2;

        internal int n;
        internal RectangleR2[] b;
        internal Link branch;

        internal RtreeR2Page(IDatabase db, IPersistent obj, RectangleR2 r)
        {
            branch = db.CreateLink<IPersistent>(card);
            branch.Length = card;
            b = new RectangleR2[card];
            setBranch(0, new RectangleR2(r), obj);
            n = 1;
            for (int i = 1; i < card; i++)
            {
                b[i] = new RectangleR2();
            }
        }

        internal RtreeR2Page(IDatabase db, RtreeR2Page root, RtreeR2Page p)
        {
            branch = db.CreateLink<IPersistent>(card);
            branch.Length = card;
            b = new RectangleR2[card];
            n = 2;
            setBranch(0, root.cover(), root);
            setBranch(1, p.cover(), p);
            for (int i = 2; i < card; i++)
            {
                b[i] = new RectangleR2();
            }
        }

        internal RtreeR2Page() { }

        internal RtreeR2Page insert(IDatabase db, RectangleR2 r, IPersistent obj, int level)
        {
            Modify();
            if (--level != 0)
            {
                // not leaf page
                int i, mini = 0;
                double minIncr = Double.MaxValue;
                double minArea = Double.MaxValue;
                for (i = 0; i < n; i++)
                {
                    double area = b[i].Area();
                    double incr = RectangleR2.JoinArea(b[i], r) - area;
                    if (incr < minIncr)
                    {
                        minIncr = incr;
                        minArea = area;
                        mini = i;
                    }
                    else if (incr == minIncr && area < minArea)
                    {
                        minArea = area;
                        mini = i;
                    }
                }
                RtreeR2Page p = (RtreeR2Page)branch[mini];
                RtreeR2Page q = p.insert(db, r, obj, level);
                if (q == null)
                {
                    // child was not split
                    b[mini].Join(r);
                    return null;
                }
                else
                {
                    // child was split
                    setBranch(mini, p.cover(), p);
                    return addBranch(db, q.cover(), q);
                }
            }
            else
            {
                return addBranch(db, new RectangleR2(r), obj);
            }
        }

        internal int remove(RectangleR2 r, IPersistent obj, int level, ArrayList reinsertList)
        {
            if (--level != 0)
            {
                for (int i = 0; i < n; i++)
                {
                    if (r.Intersects(b[i]))
                    {
                        RtreeR2Page pg = (RtreeR2Page)branch[i];
                        int reinsertLevel = pg.remove(r, obj, level, reinsertList);
                        if (reinsertLevel >= 0)
                        {
                            if (pg.n >= minFill)
                            {
                                setBranch(i, pg.cover(), pg);
                                Modify();
                            }
                            else
                            {
                                // not enough entries in child
                                reinsertList.Add(pg);
                                reinsertLevel = level - 1;
                                removeBranch(i);
                            }
                            return reinsertLevel;
                        }
                    }
                }
            }
            else
            {
                for (int i = 0; i < n; i++)
                {
                    if (branch.ContainsElement(i, obj))
                    {
                        removeBranch(i);
                        return 0;
                    }
                }
            }
            return -1;
        }

        internal void find(RectangleR2 r, ArrayList result, int level)
        {
            if (--level != 0)
            { /* this is an internal node in the tree */
                for (int i = 0; i < n; i++)
                {
                    if (r.Intersects(b[i]))
                    {
                        ((RtreeR2Page)branch[i]).find(r, result, level);
                    }
                }
            }
            else
            { /* this is a leaf node */
                for (int i = 0; i < n; i++)
                {
                    if (r.Intersects(b[i]))
                    {
                        result.Add(branch[i]);
                    }
                }
            }
        }

        internal void purge(int level)
        {
            if (--level != 0)
            { /* this is an internal node in the tree */
                for (int i = 0; i < n; i++)
                {
                    ((RtreeR2Page)branch[i]).purge(level);
                }
            }
            Deallocate();
        }

        void setBranch(int i, RectangleR2 r, IPersistent obj)
        {
            b[i] = r;
            branch[i] = obj;
        }

        void removeBranch(int i)
        {
            n -= 1;
            Array.Copy(b, i + 1, b, i, n - i);
            branch.RemoveAt(i);
            branch.Length = card;
            Modify();
        }

        RtreeR2Page addBranch(IDatabase db, RectangleR2 r, IPersistent obj)
        {
            if (n < card)
            {
                setBranch(n++, r, obj);
                return null;
            }
            else
            {
                return splitPage(db, r, obj);
            }
        }

        RtreeR2Page splitPage(IDatabase db, RectangleR2 r, IPersistent obj)
        {
            int i, j, seed0 = 0, seed1 = 0;
            double[] rectArea = new double[card + 1];
            double waste;
            double worstWaste = Double.MinValue;
            //
            // As the seeds for the two groups, find two rectangles which waste 
            // the most area if covered by a single rectangle.
            //
            rectArea[0] = r.Area();
            for (i = 0; i < card; i++)
            {
                rectArea[i + 1] = b[i].Area();
            }
            RectangleR2 bp = r;
            for (i = 0; i < card; i++)
            {
                for (j = i + 1; j <= card; j++)
                {
                    waste = RectangleR2.JoinArea(bp, b[j - 1]) - rectArea[i] - rectArea[j];
                    if (waste > worstWaste)
                    {
                        worstWaste = waste;
                        seed0 = i;
                        seed1 = j;
                    }
                }
                bp = b[i];
            }
            byte[] taken = new byte[card];
            RectangleR2 group0, group1;
            double groupArea0, groupArea1;
            int groupCard0, groupCard1;
            RtreeR2Page pg;

            taken[seed1 - 1] = 2;
            group1 = new RectangleR2(b[seed1 - 1]);

            if (seed0 == 0)
            {
                group0 = new RectangleR2(r);
                pg = new RtreeR2Page(db, obj, r);
            }
            else
            {
                group0 = new RectangleR2(b[seed0 - 1]);
                pg = new RtreeR2Page(db, branch.GetRaw(seed0 - 1), group0);
                setBranch(seed0 - 1, r, obj);
            }
            groupCard0 = groupCard1 = 1;
            groupArea0 = rectArea[seed0];
            groupArea1 = rectArea[seed1];
            //
            // Split remaining rectangles between two groups.
            // The one chosen is the one with the greatest difference in area 
            // expansion depending on which group - the rect most strongly 
            // attracted to one group and repelled from the other.
            //
            while (groupCard0 + groupCard1 < card + 1
                && groupCard0 < card + 1 - minFill
                && groupCard1 < card + 1 - minFill)
            {
                int betterGroup = -1, chosen = -1;
                double biggestDiff = -1;
                for (i = 0; i < card; i++)
                {
                    if (taken[i] == 0)
                    {
                        double diff = (RectangleR2.JoinArea(group0, b[i]) - groupArea0)
                            - (RectangleR2.JoinArea(group1, b[i]) - groupArea1);
                        if (diff > biggestDiff || -diff > biggestDiff)
                        {
                            chosen = i;
                            if (diff < 0)
                            {
                                betterGroup = 0;
                                biggestDiff = -diff;
                            }
                            else
                            {
                                betterGroup = 1;
                                biggestDiff = diff;
                            }
                        }
                    }
                }
                Debug.Assert(chosen >= 0);
                if (betterGroup == 0)
                {
                    group0.Join(b[chosen]);
                    groupArea0 = group0.Area();
                    taken[chosen] = 1;
                    pg.setBranch(groupCard0++, b[chosen], branch.GetRaw(chosen));
                }
                else
                {
                    groupCard1 += 1;
                    group1.Join(b[chosen]);
                    groupArea1 = group1.Area();
                    taken[chosen] = 2;
                }
            }
            //
            // If one group gets too full, then remaining rectangle are
            // split between two groups in such way to balance cards of two groups.
            //
            if (groupCard0 + groupCard1 < card + 1)
            {
                for (i = 0; i < card; i++)
                {
                    if (taken[i] == 0)
                    {
                        if (groupCard0 >= groupCard1)
                        {
                            taken[i] = 2;
                            groupCard1 += 1;
                        }
                        else
                        {
                            taken[i] = 1;
                            pg.setBranch(groupCard0++, b[i], branch.GetRaw(i));
                        }
                    }
                }
            }
            pg.n = groupCard0;
            n = groupCard1;
            for (i = 0, j = 0; i < groupCard1; j++)
            {
                if (taken[j] == 2)
                {
                    setBranch(i++, b[j], branch.GetRaw(j));
                }
            }
            return pg;
        }

        internal RectangleR2 cover()
        {
            RectangleR2 r = new RectangleR2(b[0]);
            for (int i = 1; i < n; i++)
            {
                r.Join(b[i]);
            }
            return r;
        }
    }
}